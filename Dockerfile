FROM lwieske/java-8:jre-8u191

RUN yum install -y docker-client

COPY . /app

WORKDIR /app

CMD java -cp /app/distributedBank.jar -DlogFile=distributedBank_$(date +%H:%M:%S).log es.upm.dit.cnvr.distributedBank.BankCore /app
