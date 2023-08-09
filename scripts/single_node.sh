#!/usr/bin/env bash
node dist/index.js start --cluster '[{ "node": 1, "port": 3000 }]' --node 1 --port 3030