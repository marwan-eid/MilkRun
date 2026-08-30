@echo off
echo ========================================================
echo Launching the Milk-Run Fleet Tracker...
echo This will route through WSL. Please keep this window open.
echo ========================================================
wsl -d Ubuntu-22.04-valeo-wsl2 -- bash -c "cd /mnt/c/Users/meid1/PC/MilkRun && ./run-all.sh"
