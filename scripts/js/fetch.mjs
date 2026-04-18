import { connect } from "node:net";

const HOST = "127.0.0.1";
const PORT = 9889;
const PATH = process.argv[2] ?? "/";

// Node.js has no built-in HTTP/3 client, so we use curl as a subprocess.
import { spawnSync } from "node:child_process";

const result = spawnSync(
  "curl",
  [
    "--http3-only",
    "-sk",
    `https://${HOST}:${PORT}${PATH}`,
  ],
  { encoding: "utf8" }
);

if (result.error) {
  console.error("Failed to run curl:", result.error.message);
  process.exit(1);
}

if (result.status !== 0) {
  console.error("curl exited with status", result.status);
  console.error(result.stderr);
  process.exit(result.status);
}

const body = result.stdout.trim();

try {
  const parsed = JSON.parse(body);
  console.log(JSON.stringify(parsed, null, 2));
} catch {
  console.log(body);
}
