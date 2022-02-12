# 시스템 패키지  설치
```bash
sudo apt update
sudo apt upgrade
sudo apt install default-jdk scala -y
```

- 참고자료: https://dreamlog.tistory.com/607
- 참고자료: https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/
- 참고자료: https://surgach.tistory.com/86

# 다운로드 
```bash
wget https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
```

# 설치
```
tar xvf spark-3.2.1-bin-hadoop3.2.tgz
sudo mv spark-3.2.1-bin-hadoop3.2/ /opt/spark 
sudo mkdir /opt/spark
# sudo tar -xf spark-3.1.2.tgz -C /opt/spark --strip-component 1
# sudo chmod -R 777 /opt/spark
```

# 환경변수 추가
```bash
# vi ~/.bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3

Reload shell:
source ~/.bashrc
```

# 실행 (Masster Server)
```
# /opt/spark/sbin/start-master.sh
start-master.sh

sudo ss -tunelp | grep 8080
```

# 워커 실행
```
start-slave.sh spark://ubuntu:7077

$ sudo updatedb
$ locate start-slave.sh
/opt/spark/sbin/start-slave.sh
```

# Spark 실행
```
pyspark
```