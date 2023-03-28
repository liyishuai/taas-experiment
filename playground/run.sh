rm -rf ./pd/*
rm -rf ./pd.log

WORKDIR=`pwd`
HOST_IP=`hostname -i`
QUORUM_SIZE=3
MYID=${WORKDIR:${#WORKDIR}-1}
CLIENT_BASE_PORT=3000
PEER_BASE_PORT=4000
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
PARA=$PARA" --initial-cluster="$(gen_member $QUORUM_SIZE $PEER_BASE_PORT)

# if ! [ $MYID == 1 ];then
#     PARA=$PARA" --join="$(gen_member $[ $MYID - 1 ] $CLIENT_BASE_PORT)
# fi
echo "PARA: "$PARA

nohup ./pd-server --name="pd$MYID" --data-dir="pd"  --log-file=pd.log $PARA 2>&1 &


