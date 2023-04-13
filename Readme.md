# Backtrader FUTU 
Support for Futu API in backtrader
**This integration is still under development and have some issues, use it for live trading take your own risk**
## What is it
-------------------------------------
**backtraderfutu** is a package to integrate FUTU API into backtrader. You should have your own Futu API account with sufficient privilege to access futu data.

**It includes the following features:**
* backtraderfutu provides a Futu DataFeed which support live data and historical data backfill
* Dockerfile for creating and running a docker container to help you kick start and have a try

Please feel free to contact me or raise issue if you have any problem.

## Docker
-------------------------------------
1. You should amend the **FutuOpenD.xml** with your own account, password and other FutuOpenD configs before you try to build the **docker image**
2. **bttool** is a simple shell script to help create/start a docker container
3. After docker container running, you may need to get into the docker container terminal, then
   ```
   telnet <FutuOpenD Host> <FutuOpenD Telnet Port>
   ```
   to input verify code if it is your first login.
   It is highly recommended to refer the details in Futu API Doc to understand Futu API's working flow and operation command.
4. FutuOpenD is under supervisord management, you can check status by
   ```
   supervisorctl status
   ```
   and start/stop by
   ```
   supervisorctl start futu
   supervisorctl stop futu
   ```
5. A test python program is **/backtrader/tests/** in docker container for testing and trial.

