#rm -rf ./pd/*
#rm -rf ./pd.log

WORKDIR=`pwd`
HOST_IP=localhost
#if hostname -i >/dev/null 2>&1; then
#  HOST_IP=$(hostname -i)
#else
#  HOST_IP=$(ipconfig getifaddr en0)
#fi
QUORUM_SIZE=5
MYID=${WORKDIR:${#WORKDIR}-1}
CLIENT_BASE_PORT=5000
PEER_BASE_PORT=6000
CLIENT_PORT=$[ $CLIENT_BASE_PORT + 10 * ${MYID} ]
PEER_PORT=$[ $PEER_BASE_PORT + 10 * ${MYID}]
echo "MYID: "$MYID
echo "CLENT_PORT: "$CLIENT_PORT
echo "PEER_PORT: "$PEER_PORT

function gen_member(){
    MEMBERS=""
    for ((i=1; i<=$1; i ++))
    do
        JOIN_PORT=$[ $2 + 10 * $i ]
        MEMBERS=$MEMBERS"pd${i}=http://${HOST_IP}:${JOIN_PORT}"
        if ! [ $i == $1 ] ;then
            MEMBERS=$MEMBERS","
        fi
    done
    echo $MEMBERS
}

PARA=""
PARA=$PARA" --client-urls=http://${HOST_IP}:${CLIENT_PORT}"" --peer-urls=http://${HOST_IP}:${PEER_PORT}"
if [ "$1" = "start" ]; then
     PARA=$PARA" --initial-cluster="$(gen_member $QUORUM_SIZE $PEER_BASE_PORT $1)
fi
echo "PARA: "$PARA

nohup ./pd-server --name="pd$MYID" --data-dir="pd"  --log-file=pd.log $PARA 2>&1 &


