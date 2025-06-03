Example commands:

node mqttPubTest.js --broker mqtt://10.17.10.250:1883 --username user1 --password g.rDJ8i.oxKCA8Zf- --topic testtopic/test --qos 2 --sessionExpiry 120 --messageExpiry 180 --rate 1 --numberMessages 30 --statsInterval 1 --payloadSize 10

node mqttSubTest.js   --broker mqtt://10.17.10.250:1885   --clientId subscriber01   --username user1   --password LxUYCw@VUVzKEr6K_   --topic testtopic/test   --qos 2   --sessionExpiry 120   --clearStats 0

node sharedsubtester.js --broker mqtt://10.17.10.250:1883 --logFile ./logs/shared-test.log --csvFile ./reports/shared-report.csv --username user1 --password 'LxUYCw@VUVzKEr6K_' --topic testtopic/test --qos 2 --groups 2 --subsPerGroup 3  --publishers 1  --messagesPerPub 50  --edgeDisconnect false

node inflighttester.js --broker mqtt://10.17.10.250:1883 --username user1 --password 'password' --topic testtopic/test --qos 2 --sessionExpiry 120 --subs 3 --publishers 2 --messagesPerPub 20 --logFile ./logs/inflight-test.log --csvFile ./reports/inflight-report.csv
