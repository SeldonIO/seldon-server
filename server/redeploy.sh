#!/bin/bash
set -e
cd ${0%/*}
[ -z "$TOMCAT_HOME" ] && { echo "Need to set \$TOMCAT_HOME"; exit 1; }
type -P terminal-notifier &>/dev/null || brew install terminal-notifier > /dev/null
command -v memcached >/dev/null 2>&1 || { echo >&2 "Please install memcached before running this command. For example (MacOSX) run 'brew install memcached'"; exit 1; }
command -v zkServer >/dev/null 2>&1 || { echo >&2 "Please install zookeeper before running this script. For example (MacOSX) run 'brew install zookeeper'"; exit 1; }
echo     "%%%%%%%%%%%%%%%%          Found TOMCAT_HOME at $TOMCAT_HOME              %%%%%%%%%%%%%%%%%%%%%%%%"
usertext='<tomcat-users>
  <role rolename="tomcat"/>
  <role rolename="manager-script"/>
  <role rolename="manager-gui"/>
  <user username="tomcat" password="tomcat" roles="tomcat,manager-gui,manager-script"/>
</tomcat-users>'
echo "$usertext" > $TOMCAT_HOME/conf/tomcat-users.xml
running=`ps aux | grep $TOMCAT_HOME | wc -l`
touch $TOMCAT_HOME/bin/setenv.sh
pidFileLine=`grep CATALINA_PID $TOMCAT_HOME/bin/setenv.sh | wc -l`
if [[ pidFileLine -eq 0 ]] ; then
	echo "CATALINA_PID=$TOMCAT_HOME/tomcat.pid" >> $TOMCAT_HOME/bin/setenv.sh
	if [[ $running -gt 1 ]] ; then
		echo "Can't find tomcat pid file, please stop tomcat (kill it) and restart this script"
		exit 1
	fi
else
	
	pidFile=`sed -n -e 's/^.*CATALINA_PID=//p' $TOMCAT_HOME/bin/setenv.sh`

	if [ -f $pidFile ]; then
		tomcat_pid=`cat $pidFile`
	fi
fi
if [[ $running -gt 1 ]] ; then
    echo '%%%%%%%%%%%%%%%%%         Tomcat is currently runnning ... stopping it                                              %%%%%%%%%%%%%%%%%%%%%%%%'
    $TOMCAT_HOME/bin/shutdown.sh > /dev/null
    sleep 5
    if ps -p $tomcat_pid 
    then
	    echo "TOMCAT DIDN'T SHUT DOWN PROPERLY -- TRYING TO KILL IT."
	    kill $tomcat_pid
    fi
fi

echo     '%%%%%%%%%%%%%%%%%         Removing any deployed copies of the seldon-server                                                %%%%%%%%%%%%%%%%%%%%%%%%'
rm -rf $TOMCAT_HOME/webapps/seldon-server*
echo     '%%%%%%%%%%%%%%%%          Stopping zookeeper if it is running                                                           %%%%%%%%%%%%%%%%%%%%%%%'
zkServer stop
echo     '%%%%%%%%%%%%%%%%          Stopping memcached if it is running                                                           %%%%%%%%%%%%%%%%%%%%%%%'
[ -f $HOME/memcached.pid ] && if ! kill $(cat $HOME/memcached.pid) > /dev/null 2>&1; then
    rm $HOME/memcached.pid
fi




echo     '%%%%%%%%%%%%%%%%          Starting zookeeper                                                                            %%%%%%%%%%%%%%%%%%%%%%%%'
zkServer start
echo     '%%%%%%%%%%%%%%%%          Starting memcached                                                                            %%%%%%%%%%%%%%%%%%%%%%%%'
memcached -d -P $HOME/memcached.pid
echo     '%%%%%%%%%%%%%%%%         Checking configuration                                                                         %%%%%%%%%%%%%%%%%%%%%%%%'
echo     '%%%%%%%%%%%%%%%%         Redeploying seldon-server                                                                         %%%%%%%%%%%%%%%%%%%%%%%%'
$TOMCAT_HOME/bin/catalina.sh jpda start > /dev/null
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"
mvn -e org.apache.tomcat.maven:tomcat7-maven-plugin:redeploy -Dconfig.version=local -Dtomcat.username=tomcat -Dtomcat.password=tomcat -DskipTests
sleep 5
running=`wget -qO- localhost:8080/seldon-server | grep 'Welcome to the Seldon API'| wc -l`
if [[ running -eq 0 ]] ; then
    echo '%%%%%%%%%%%%%%%%         Something went wrong, seldon-server not deployed succesfully. Please check the logs               %%%%%%%%%%%%%%%%%%%%%%%%'
else
    echo '%%%%%%%%%%%%%%%%         API server deployed succesfully!                                                               %%%%%%%%%%%%%%%%%%%%%%%%'
fi
