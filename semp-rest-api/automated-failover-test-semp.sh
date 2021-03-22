#!/usr/bin/env sh

# https://docs.solace.com/API-Developer-Online-Ref-Documentation/swagger-ui/config/index.html

ROUTER=localhost:8080
ADMIN_USER=admin
ADMIN_PW=admin
VPN=default
OUTAGE_LENGTH_SECONDS=5

VPN=default
CLIENT_USERNAME=default
CLIENT_PROFILE=default
ACL_PROFILE=default

#echo Getting hostname via SEMPv1...
#OUTPUT=$(curl -s -u $ADMIN_USER:$ADMIN_PW http://$ROUTER/SEMP -X POST -d '<rpc><show><hostname/></show></rpc>' | perl -ne ' if (m|<hostname>(.*?)</hostname>|) { print "$1"; } ')
# might not be global admin priveleges (e.g. Solace CLoud)

CUR_SPOOL=$(curl -s -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/default?select=maxMsgSpoolUsage" -X GET -H "Content-type:application/json" | perl -ne ' if (/"maxMsgSpoolUsage":(\d+)/) { print "$1"; } ')
echo CUR SPOOL = $CUR_SPOOL

echo About to run some simple error case tests on Solace broker $OUTPUT at $ROUTER

# bounce the Message VPN to disable all client connections and such for a few seconds
echo About to shutdown $VPN VPN for $OUTAGE_LENGTH_SECONDS seconds...
if ! curl -f -s -S -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/$VPN" -X PATCH -H "Content-type:application/json" -d '{"enabled":false}' > /dev/null; then
    echo " X ERROR! Could not shutdown $VPN VPN. Exiting."
    exit 1
else
    echo " + Success! $VPN VPN is shutdown."
fi
sleep $OUTAGE_LENGTH_SECONDS

echo About to enable $VPN VPN...
if ! curl -f -s -S -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/$VPN" -X PATCH -H "Content-type:application/json" -d '{"enabled":true}' > /dev/null; then
    echo " X ERROR! Could not enable $VPN VPN. Beware, VPN might be left in a shutdown state. Exiting."
    exit 2
else
    echo " + Success! $VPN VPN is enabled."
fi
exit 0


# set the message spool to 0bounce the Message VPN to disable all client connections and such for a few seconds
echo About to disable all persistent publishing into $VPN VPN for $OUTAGE_LENGTH_SECONDS seconds...

if ! curl -f -s -S -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/$VPN" -X PATCH -H "Content-type:application/json" -d '{"maxMsgSpoolUsage":0}' > /dev/null; then
    echo " X ERROR! Could not shutdown $VPN VPN. Exiting."
    exit 1
else
    echo " + Success! $VPN VPN is shutdown."
fi
sleep $OUTAGE_LENGTH_SECONDS

echo About to put message spool back to $CUR_SPOOL MB in $VPN VPN...
if ! curl -f -s -S -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/$VPN" -X PATCH -H "Content-type:application/json" -d '{"maxMsgSpoolUsage":$CUR_SPOOL}' > /dev/null; then
    echo " X ERROR! Could not enable $VPN VPN. Beware, VPN might be left in a shutdown state. Exiting."
    exit 2
else
    echo " + Success! VPN $VPN is enabled."
fi




# now let's add an ACL publish issue
echo About to add a publish ACLs to acl-profile $ACL_PROFILE for $OUTAGE_LENGTH_SECONDS seconds...
exit 3
if ! curl -f -s -S -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/$VPN" -X PATCH -H "Content-type:application/json" -d '{"enabled":false}' > /dev/null; then
    echo " X ERROR! Could not shutdown VPN. Exiting."
    exit 1
else
    echo " + Success! VPN is shutdown."
fi
sleep $OUTAGE_LENGTH_SECONDS

echo About to enable the VPN...
if ! curl -f -s -S -u $ADMIN_USER:$ADMIN_PW "http://$ROUTER/SEMP/v2/config/msgVpns/$VPN" -X PATCH -H "Content-type:application/json" -d '{"enabled":true}' > /dev/null; then
    echo " X ERROR! Could not enable VPN. Beware, VPN might be left in a shutdown state. Exiting."
    exit 2
else
    echo " + Success! VPN is enabled."
fi





