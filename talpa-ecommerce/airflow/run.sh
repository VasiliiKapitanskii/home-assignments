#!/bin/bash
branch_name="$(git symbolic-ref HEAD 2>/dev/null)" ||
branch_name="(unnamed branch)"     # detached HEAD

branch_name="${branch_name##refs/heads/}"
export BRANCH_NAME="$branch_name"
docker-compose -f docker-compose.yml up -d --build