# TeraStatisticsAnalyser

Rust version of this https://github.com/neowutran/TeraDatabaseAnalysor 

Analyse the logs recolted by Shinra meter and output some statistics

Linux only due to some issue with the LZMA decompressor: https://github.com/neowutran/tera_statistics_analyser/blob/master/src/main.rs#L258 
If someone know how to fix that 

# Usage
```sh
tera_statistics_analyser --source ~/QubesIncoming/hubic-tera/t/ --target /tmp/t/
```
output 
```
[user@dev t]$ ls -lsa
total 28
0 drwxrwxr-x 10 user user 360 Mar 18 15:50 .
0 drwxrwxrwt 20 root root 620 Mar 18 15:49 ..
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 EU
4 -rw-rw-r--  1 user user 179 Mar 18 15:52 EU.txt
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 JP
4 -rw-rw-r--  1 user user 176 Mar 18 15:52 JP.txt
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 KR
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 KR-PTS
0 -rw-rw-r--  1 user user   0 Mar 18 15:52 KR-PTS.txt
4 -rw-rw-r--  1 user user 175 Mar 18 15:52 KR.txt
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 NA
4 -rw-rw-r--  1 user user 176 Mar 18 15:52 NA.txt
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 RU
4 -rw-rw-r--  1 user user 174 Mar 18 15:52 RU.txt
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 THA
4 -rw-rw-r--  1 user user 152 Mar 18 15:52 THA.txt
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 TW
4 -rw-rw-r--  1 user user 168 Mar 18 15:52 TW.txt
[user@dev t]$ ls -lsa ./EU
total 4
0 drwxrwxr-x  2 user user  60 Mar 18 15:50 .
0 drwxrwxr-x 10 user user 360 Mar 18 15:50 ..
4 -rw-rw-r--  1 user user 179 Mar 18 15:52 2018-3.txt
[user@dev t]$ cat ./EU/2018-3.txt 
Archer:11514
Berserker:7379
Brawler:11500
Common:345
Gunner:4363
Lancer:9231
Mystic:13110
Ninja:6844
Priest:9800
Reaper:3082
Slayer:5217
Sorcerer:8474
Valkyrie:7414
Warrior:14115
```

# What
- Find recursively all .xz files from the source folder
- Create a create of each of those file. The thread decompressed them and parse them as Json
- For each of those json, create a new processing thread. Those threads store statistics inside sqlite in memory database.
- Export statistics to files


