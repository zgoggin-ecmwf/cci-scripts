#!/bin/bash

# this script can be used to configure OS
# environment variables to allow access to
# CCI via a keystone credential
# usage: ./cci-setter.sh
# contact: zachary.goggin@ecmwf.int

# check that openstack CLI is available
if ! command -v openstack >/dev/null 2>&1; then
    echo "did not find an OSC install."
    exit 1
fi

# set some interim params
CACHE_PASS="/tmp/cci-keystone-pass"
CACHE_USER="/tmp/cci-keystone-user"

export OS_USER_DOMAIN_NAME="Default"
export OS_PROJECT_DOMAIN_ID="Default"
export OS_REGION_NAME="RegionOne"
export OS_INTERFACE="public"
export OS_IDENTITY_API_VERSION=3

# If cached user exists and is non-empty, use it
if [ -s "$CACHE_USER" ]; then
    OS_USERNAME=$(cat "$CACHE_USER")
    echo "cache file '$CACHE_USER' exists, using."
else
    # Otherwise prompt and cache it
    read -p "Please enter keystone username: " OS_USERNAME
    echo
    echo "$OS_USERNAME" > "$CACHE_USER"
    chmod 600 "$CACHE_USER"
fi
export OS_USERNAME

# TODO: zgoggin - duplicating here :/

# If cached password exists and is non-empty, use it
if [ -s "$CACHE_PASS" ]; then
    OS_PASSWORD=$(cat "$CACHE_PASS")
    echo "cache file '$CACHE_PASS' exists, using."
else
    # Otherwise prompt and cache it
    read -sp "Please enter keystone p\$ for '$OS_USERNAME': " OS_PASSWORD
    echo
    echo "$OS_PASSWORD" > "$CACHE_PASS"
    chmod 600 "$CACHE_PASS"
fi
export OS_PASSWORD

# pick a CCI
echo ""
PS3="Please pick a cloud: "
options=("cci1" "cci2")
select opt in "${options[@]}"
do
  case $opt in
    "cci1")
      echo "using cci-1 env vars"
      export OS_AUTH_URL="https://auth.os-api.cci1.ecmwf.int"
      export OS_PROJECT_ID="4ad6089317d54dc2b0ba8e71c307c5e9" # zachary project!
      break
      ;;
    "cci2")
      echo "using cc-2 env vars"
      export OS_AUTH_URL="https://auth.os-api.cci2.ecmwf.int"
      export OS_PROJECT_ID="f6277d834e834d5cb5e62645cd7d7664" # zachary project!
      break
      ;;
    *)
      echo "invalid option $REPLY"
      ;;
  esac
done

# get all projects available with
# current keystone creds
projects=()
while IFS= read -r name; do
    projects+=("$name")
done < <(openstack project list -f json | jq -r '.[].Name')

# export a specific project by its ID
PS3="Please pick a project: "
select proj in "${projects[@]}"; do
  if [ -n "$proj" ]; then
    proj_id=$(openstack project list -f json \
        | jq -r --arg NAME "$proj" '.[] | select(.Name==$NAME) | .ID')

    echo "Using project: $proj"
    echo "Project ID: $proj_id"

    export OS_PROJECT_ID="$proj_id"
    unset OS_PROJECT_NAME

    break
  else
    echo "invalid option $REPLY"
  fi
done
