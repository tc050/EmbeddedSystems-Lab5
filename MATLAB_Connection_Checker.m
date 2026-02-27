clear; clc;

% ---- SET THESE ----
pi_ip = '169.254.239.106';   % Pi IPv4 (use CHAR quotes)
pi_port = 5005;           % Pi listening port
matlab_port = 5006;       % MATLAB local port to receive replies

% ---- Create UDP port ----
u = udpport("datagram","IPV4","LocalPort",matlab_port);
fprintf("MATLAB UDP bound to local port %d\n", matlab_port);

% ---- Optional ping check ----
[status, out] = system(['ping -n 1 ' pi_ip]);
if status == 0
    fprintf("PING OK\n");
else
    fprintf("PING FAILED (may be blocked). Output:\n%s\n", out);
end

% ---- Handshake payload (IMPORTANT: char -> uint8) ----
hello = uint8(sprintf('HELLO,%d\n', matlab_port));  % char -> uint8 safely

% ---- Try sending HELLO ----
fprintf("Sending HELLO to %s:%d ...\n", pi_ip, pi_port);
try
    write(u, hello, pi_ip, pi_port);
    fprintf("HELLO sent.\n");
catch ME
    fprintf(2, "SEND FAILED:\n%s\n", ME.message);
    fprintf("Debug tips:\n");
    fprintf(" - Confirm pi_ip/ports\n");
    fprintf(" - Confirm MATLAB has permission to bind LocalPort=%d\n", matlab_port);
    fprintf(" - Try changing matlab_port to 5007 in both sides\n");
    rethrow(ME);
end

% ---- Wait for ACK ----
fprintf("Waiting for ACK (2 seconds)...\n");
t0 = tic;
got_ack = false;

while toc(t0) < 2.0
    n = u.NumDatagramsAvailable;
    if n > 0
        d = read(u, n, "uint8");
        last = d(end);
        msg = strtrim(char(last.Data));  % bytes -> char
        fprintf("Received from %s:%d : '%s'\n", last.SenderAddress, last.SenderPort, msg);

        if startsWith(msg, "ACK")
            got_ack = true;
            break;
        end
    end
    pause(0.01);
end

if ~got_ack
    error("No ACK received. Pi script not replying to this PC/port or packets blocked.");
else
    fprintf("Handshake OK ✅\n");
end