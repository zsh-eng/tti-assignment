#!/usr/bin/bash
# Regenerates the kitex code
cd ./rpc-server
kitex -module "github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server" -service imservice ../idl_rpc.thrift
cp -r ./kitex_gen ../http-server 