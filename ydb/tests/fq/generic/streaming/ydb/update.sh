#!/bin/bash -x
trap "" PIPE
# requires $compiler_path and $ydb_path and $PROFILE environment variables
# 
# Sample lists
# excerpt from <a href='https://db-ip.com'>IP Geolocation by DB-IP</a>
cat > dbip-city-ipv6.csv << EOF
2000::,2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
2001::,2001::ffff:ffff:ffff:ffff:ffff:ffff,US,California,,Los Angeles (Westchester),,33.9829,-118.405,
2001:1::,2001:1:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
2001:2::,2001:2::ffff:ffff:ffff:ffff:ffff,US,California,,Los Angeles (Westchester),,33.9829,-118.405,
2001:2:1::,2001:2:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
2c0f:ffc8:0:0:0:1::,2c0f:ffc8:ffff:ffff:ffff:ffff:ffff:ffff,ZA,Western Cape,,Cape Town (Observatory),,-33.9432,18.4726,
2c0f:ffc9::,2c0f:ffcf:ffff:ffff:ffff:ffff:ffff:ffff,MU,Plaines Wilhems,,Ebene CyberCity,,-20.2431,57.4962,
2c0f:ffd0::,2c0f:ffd0:ffff:ffff:ffff:ffff:ffff:ffff,ZA,Gauteng,,Midrand (Halfway House),,-26.0089,28.1222,
2c0f:ffd1::,2c0f:ffd7:ffff:ffff:ffff:ffff:ffff:ffff,MU,Plaines Wilhems,,Ebene CyberCity,,-20.2431,57.4962,
2c0f:ffd8::,2c0f:ffd8:ffff:ffff:ffff:ffff:ffff:ffff,ZA,Gauteng,,Sandton (Bryanston),,-26.0505,28.0249,
2c0f:ffd9::,2c0f:ffe7:ffff:ffff:ffff:ffff:ffff:ffff,MU,Plaines Wilhems,,Ebene CyberCity,,-20.2431,57.4962,
2c0f:ffe8::,2c0f:ffe8:ffff:ffff:ffff:ffff:ffff:ffff,NG,Sokoto,,Dunde,,13.1167,5.66667,
2c0f:ffe9::,2c0f:ffef:ffff:ffff:ffff:ffff:ffff:ffff,MU,Plaines Wilhems,,Ebene CyberCity,,-20.2431,57.4962,
2c0f:fff0::,2c0f:fff0:ffff:ffff:ffff:ffff:ffff:ffff,NG,FCT,,Abuja (Wuse),,9.065,7.47379,
2c0f:fff1::,2c0f:ffff:ffff:ffff:ffff:ffff:ffff:ffff,MU,Plaines Wilhems,,Ebene CyberCity,,-20.2431,57.4962,
2c10::,2dff:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
2e00::,2fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff,DE,Hesse,,Frankfurt am Main,,50.1109,8.68213,
2c0f:fb90::,2c0f:fb90:ffff:ffff:ffff:ffff:ffff:ffff,MZ,Maputo City,,"Maputo (Polana Cimento""A"")",,-25.977,32.5939,
EOF
cat > dbip-city-ipv4.csv << EOF
1.0.0.0,1.0.0.255,AU,Queensland,,South Brisbane,,-27.4767,153.017,
1.0.1.0,1.0.3.255,CN,Fujian,,Wenquan,,26.0998,119.297,
1.0.4.0,1.0.7.255,AU,Victoria,,Narre Warren,,-38.0268,145.301,
1.0.8.0,1.0.15.255,CN,Guangdong,,Guangzhou,,23.1317,113.266,
1.0.16.0,1.0.16.255,JP,Tokyo,,Chiyoda,,35.6916,139.768,
1.0.17.0,1.0.31.255,JP,Tokyo,,Shinjuku (1-chome),,35.6944,139.703,
1.0.32.0,1.0.63.255,CN,Guangdong,,Guangzhou,,23.1317,113.266,
1.0.64.0,1.0.79.255,JP,Hiroshima,,Hiroshima,,34.4002,132.475,
1.0.80.0,1.0.80.255,JP,Shimane,,Matsue,,35.4833,133.05,
1.0.81.0,1.0.85.255,JP,Hiroshima,,Hiroshima,,34.4002,132.475,
1.0.86.0,1.0.86.255,JP,Tottori,,Tottori-shi,,35.5,134.233,
1.0.87.0,1.0.95.255,JP,Hiroshima,,Hiroshima,,34.4002,132.475,
1.0.96.0,1.0.96.255,JP,Tottori,,Sakaiminato,,35.5377,133.231,
1.0.97.0,1.0.101.255,JP,Hiroshima,,Hiroshima,,34.4002,132.475,
223.255.128.0,223.255.134.255,HK,Kwun Tong,,Kowloon Bay,,22.324,114.204,
223.255.135.0,223.255.135.255,HK,Kwun Tong,,Kwun Tong,,22.3118,114.222,
223.255.136.0,223.255.145.255,HK,Kwun Tong,,Kowloon Bay,,22.324,114.204,
223.255.146.0,223.255.146.255,HK,Central and Western,,Central,,22.283,114.158,
223.255.147.0,223.255.183.255,HK,Kwun Tong,,Kowloon Bay,,22.324,114.204,
223.255.184.0,223.255.184.255,HK,Kowloon City,,Sung Wong Toi,,22.3247,114.191,
223.255.185.0,223.255.189.255,HK,Kwun Tong,,Kowloon Bay,,22.324,114.204,
223.255.190.0,223.255.190.255,HK,Central and Western,,The Belcher's,,22.2848,114.133,
223.255.191.0,223.255.191.255,HK,Kwun Tong,,Kowloon Bay,,22.324,114.204,
223.255.192.0,223.255.192.15,KR,Gyeonggi-do,,Seongnam-si (Dunchon-daero),,37.4309,127.155,
223.255.192.16,223.255.207.255,KR,Jeollanam-do,,Naju-si,,35.0161,126.711,
223.255.208.0,223.255.223.255,KR,Gyeonggi-do,,Seongnam-si (Bundang-gu),,37.3827,127.119,
223.255.224.0,223.255.224.255,ID,Riau,,Batam,,1.14937,104.025,
223.255.225.0,223.255.226.255,ID,Jakarta,,Jakarta,,-6.22423,106.817,
223.255.227.0,223.255.227.255,ID,West Sumatra,,Tabing,,-0.87203,100.343,
223.255.228.0,223.255.228.255,ID,Jakarta,,Jakarta,,-6.22423,106.817,
223.255.229.0,223.255.229.79,ID,West Java,,Depok,,-6.4,106.819,
223.255.229.80,223.255.231.127,ID,Jakarta,,Jakarta,,-6.22423,106.817,
223.255.231.128,223.255.231.255,ID,West Sumatra,,Padang,,-0.94924,100.354,
223.255.232.0,223.255.233.255,AU,Northern Territory,,Darwin,,-12.4637,130.844,
223.255.234.0,223.255.234.255,AU,Northern Territory,,Winnellie,,-12.4321,130.871,
223.255.235.0,223.255.235.255,AU,Northern Territory,,Darwin,,-12.4637,130.844,
223.255.236.0,223.255.239.255,CN,Beijing,,Beijing,,39.9042,116.407,
223.255.240.0,223.255.241.255,HK,Yau Tsim Mong,,Mong Kok,,22.3183,114.167,
223.255.242.0,223.255.242.255,HK,Kowloon,,Hong Kong,,22.3193,114.169,
223.255.243.0,223.255.243.255,HK,Yau Tsim Mong,,Mong Kok,,22.3183,114.167,
223.255.244.0,223.255.247.255,IN,Gujarat,,Rajkot (Jagnath Plot),,22.2933,70.7907,
223.255.248.0,223.255.251.255,HK,Kowloon,,Hong Kong,,22.3193,114.169,
223.255.252.0,223.255.253.255,CN,Fujian,,Wenquan,,26.0998,119.297,
223.255.254.0,223.255.254.255,SG,,,Singapore (Downtown Core),,1.27929,103.85,
223.255.255.0,223.255.255.255,AU,Queensland,,South Brisbane,,-27.4767,153.017,
EOF

# compile dicitonaries
# save payload dictionary in payload.csv, loads previous payload from last-payload.csv

cat dbip-city-ipv4.csv dbip-city-ipv6.csv | \
$compiler_path/ip-dict-compiler \
    `[ ! -e last-payload.json ] || --input-payload last-payload.csv` \
    --output-format json \
    --output-payload payload.csv \
    --ipv4-prefix-size 8 \
    --ipv6-prefix-size 16 \
    > geoip-trie.json

# create tables

$ydb_path/ydb -p ${PROFILE} yql -s '
    CREATE TABLE `geodb` (`prefix` STRING, `ipdict` STRING, PRIMARY KEY(`prefix`));
        COMMIT;
    CREATE TABLE `geodbmeta` (`ip_key` Int64, `country` STRING, `state1` STRING, `state2` STRING, `city` STRING, `lattitude` Double, `longitude` Double, `tz` STRING, PRIMARY KEY(`ip_key`));
        COMMIT;
'

# import data

$ydb_path/ydb -p ${PROFILE} import file json -p geodb < geoip-trie.json
../csv2json/csv2json < payload.csv \
    ip_key:i country:s? state1:s? state2:s? city:s? zip:s? lattitude:f? longitude:f? tz:s? \
    | tee payload.json | \
$ydb_path/ydb -p ${PROFILE} import file json -p geodbmeta

mv payload.csv last-payload.csv
