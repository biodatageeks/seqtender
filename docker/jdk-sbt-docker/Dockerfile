FROM biodatageeks/jdk-sbt:8.0.265.hs-adpt-1.3.10

RUN apt-get update && \
    apt-get remove docker docker-engine docker.io containerd runc && \
    apt-get -qq -y install \
        apt-transport-https \
        ca-certificates \
        curl \
        gnupg-agent \
        software-properties-common && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - && \
    apt-key fingerprint 0EBFCD88 && \
    add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable" && \
    apt-get -qq -y install docker-ce docker-ce-cli containerd.io