% plant_temperature_udp_dataset.m
% Temperature plant controlled by Raspberry Pi over UDP (dataset-ready)

clear; clc;

% ---------- Plant parameters ----------
Tamb = 20;
T = 20;
k_heat = 0.8;
k_cool = 0.15;
dist = -0.5; % disturbance injection to control output

% --------- Configuration parameters ----------
dt = 0.02;                 % 50 Hz plant tick (also deadline)
stress_level = false;          % in ms
scheduler = 'FCFS';

duration = 30;             % seconds
steps = round(duration/dt);

% ---------- Target ----------
setpoint = 22;
measurement_jitter = false;
disturbance_injection = false;

% ---------- UDP settings ----------
pi_ip = '169.254.239.106';    % Pi IP (CHAR)
pi_port = 5005;            % Pi listens here
matlab_port = 5006;         % MATLAB listens here

u = udpport("datagram","IPV4","LocalPort",matlab_port);

% ---------- Handshake ----------
fprintf("Checking connection (HELLO->ACK)...\n");
hello = uint8(sprintf('HELLO,%d\n', matlab_port));
write(u, hello, pi_ip, pi_port);

got_ack = false;
t0 = tic;
while toc(t0) < 2.0
    if u.NumDatagramsAvailable > 0
        d = read(u, u.NumDatagramsAvailable, "uint8");
        msg = strtrim(char(d(end).Data));
        if startsWith(msg,"ACK")
            got_ack = true;
            break;
        end
    end
    pause(0.01);
end
if ~got_ack
    error("No ACK from Pi. Check Pi script running and UDP ports.");
end
fprintf("Handshake OK ✅\n");

% ---------- Logs ----------
log = table('Size',[steps 17], ...
    'VariableTypes', ["double","double","double","double","double","double","double","logical", ...
                      "string","string","double","double","double","double","logical","double","double"], ...
    'VariableNames', ["cycle_id","timestamp_start_ms","timestamp_end_ms","dt_target_ms","loop_latency_ms","jitter_ms","deadline_ms","late_flag","controller_mode", ...
                      "scenario_id","r","y","e","u","sat_flag","rx_seq","tx_seq"]);

metrics = table('Size',[1 9], ...
    'VariableTypes', ["double","double","double","double","double","double","uint64","double","double"], ...
    'VariableNames', ["overshoot_perc","steady_state_error","settling_time","rise_time","avg_loop_latency_ms","p95_loop_latency_ms","deadline_miss","miss_rate","jitter"]);

fprintf("Starting plant loop (%d steps)...\n", steps);

seq = 0;
start_wall = tic;

for i = 1:steps
    seq = seq + 1;
    t_sim = (i-1)*dt;

    % ---- measurement jitter ----
    if measurement_jitter == true
        if rand(1) > 5
            m_jitter = rand(1)/2; % measurement noise
        else
            m_jitter = -rand(1)/2; % measurement noise
        end
        T_toController = T + m_jitter;
    else
        T_toController = T; % no jitter
    end

    % ---- Build sensor packet ----
    % Format: DATA,seq,t_sim,temp,setpoint,dt
    msg_out = sprintf("DATA,%d,%.6f,%.3f,%.3f,%.4f\n", seq, t_sim, T_toController, setpoint, dt);
    bytes_out = uint8(char(msg_out));

    wall_send = toc(start_wall) * 1000.0;
    write(u, bytes_out, pi_ip, pi_port);

    % ---- Receive controller command (expect: CMD,seq,t_sim,u_cmd,pi_latency_ms) ----
    u_cmd = 0.0;
    wall_recv = NaN;
    rx_bytes = 0;
    rx_age_ms = NaN;
    notes = "";
    jitter = 0.0;

    t_wait = tic;
    got = false;

    while toc(t_wait) < 0.020  % 10ms max wait
        n = u.NumDatagramsAvailable;
        if n > 0
            d = read(u, n, "uint8");
            last = d(end);
            msg_in = strtrim(char(last.Data));
            rx_bytes = numel(last.Data);

            parts = split(msg_in, ",");
            if numel(parts) >= 5 && parts{1} == "CMD"
                seq_in = str2double(parts{2});
                u_in = str2double(parts{4});

                if seq_in == seq && ~isnan(u_in)
                    u_cmd = u_in;
                    got = true;
                    wall_recv = toc(start_wall) * 1000.0;
                    rx_age_ms = (wall_recv - wall_send);
                    jitter = rx_age_ms - (dt * 1000.0);
                    break;
                end
            end
        end
    end

    if ~got
        notes = "no_cmd_timeout";
    end

    % ---- Clamp u ----
    u_cmd = max(0.0, min(1.0, u_cmd));

    % ---- Plant update ----
    if disturbance_injection == true
        if i > 1500/3 % inject disturbance after 10 seconds
            dT = (k_heat*(u_cmd + dist) - k_cool*(T - Tamb)) * dt;
        else
            dT = (k_heat*u_cmd - k_cool*(T - Tamb)) * dt;
        end
    else
        dT = (k_heat*u_cmd - k_cool*(T - Tamb)) * dt;
    end
    T = T + dT;
    e = setpoint - T;

    % ---- Timing / scheduling metrics ----
    loop_elapsed = toc(start_wall) - t_sim;     % positive means running late
    deadline_s = dt * 1000.0;                            % nominal deadline
    dropped = double(~got);

    % ---- Save row ----
    log.cycle_id(i) = seq;
    log.timestamp_start_ms(i) = wall_send;
    log.timestamp_end_ms(i) = wall_recv;
    log.dt_target_ms(i) = deadline_s;
    log.loop_latency_ms(i) = rx_age_ms;
    log.jitter_ms(i) = jitter;
    log.deadline_ms(i) = deadline_s;
    if rx_age_ms > deadline_s
        log.late_flag(i) = 1;
    else
        log.late_flag(i) = 0;
    end
    log.controller_mode(i) = 'PID'; % change manually
    log.scenario_id(i) = 'none yet'; % change manually
    log.r(i) = setpoint;
    log.y(i) = T;
    log.e(i) = e;
    log.u(i) = u_cmd;
    log.sat_flag(i) = 0;
    log.rx_seq(i) = rx_bytes;
    log.tx_seq(i) = numel(bytes_out);

    % ---- Pace to real-time-ish ----
    elapsed = toc(start_wall);
    pause(max(0, t_sim - elapsed));
end

% ---- Save logs ----
writetable(log, "cycle.csv");
disp("Saved cycle.csv");

% ---- Plots ----
time = (log.cycle_id - 1) * dt;

figure; plot(time, log.y); hold on; plot(time, log.r, 'LineStyle', '--');
xlabel('t (s)'); ylabel('Temp (°C)'); title('Temperature vs Setpoint');

figure; plot(time, log.u);
xlabel('t (s)'); ylabel('u'); title('Control Output');
figure; plot(time, log.loop_latency_ms);
xlabel('t (s)'); ylabel('Latency (ms)'); title('Latency of UDP Round Trip');

% ---- Metric Logs ----
metric_data = stepinfo(log.y, time, log.y(end), log.y(1));

metrics.overshoot_perc(1) = metric_data.Overshoot;
metrics.steady_state_error(1) = abs(log.r(end) - mean(log.y(1400:end)));
metrics.settling_time(1) = metric_data.SettlingTime;
metrics.rise_time(1) = metric_data.RiseTime;
metrics.avg_loop_latency_ms(1) = mean(rmmissing(log.loop_latency_ms(:)));
sorted_latency = sort(rmmissing(log.loop_latency_ms(:)));
metrics.p95_loop_latency_ms(1) = mean(sorted_latency((steps*0.95):end));
metrics.deadline_miss(1) = nnz(log.late_flag(:));
metrics.miss_rate(1) = metrics.deadline_miss(1) / steps * 100;
metrics.jitter(1) = mean(log.jitter_ms);

% ---- Save Metrics ----
writetable(log, "metrics.csv");
disp("Saved metrics.csv");
