WORKDIR=`pwd`
HOST_IP=`hostname -i`
QUORUM_SIZE=$2
MYID=$3
RESTART=$4
CLIENT_BASE_PORT=3000
PEER_BASE_PORT=4000
CLIENT_PORT=$[ $CLIENT_BASE_PORT  ]
PEER_PORT=$[ $PEER_BASE_PORT  ]
echo "MYID: "$MYID
echo "CLENT_PORT: "$CLIENT_PORT
echo "PEER_PORT: "$PEER_PORT
#IPLIST=#{}
IPLIST=$1
function gen_member(){
    OLD_IFS="$IFS"
    IFS=","
    IPADRR=($3)
    IFS="$OLD_IFS"
    PARA=""
    #echo {$IPADDR}
    MEMBERS=""
    for ((i=1; i<=$1; i ++))
    do
        JOIN_PORT=$2
        MEMBERS=$MEMBERS"pd${i}=http://${IPADRR[$i-1]}:$2"
        if ! [ $i == $1 ] ;then
            MEMBERS=$MEMBERS","
        fi
    done
    echo $MEMBERS
}

PARA=$PARA" --client-urls=http://${HOST_IP}:${CLIENT_PORT}"" --peer-urls=http://${HOST_IP}:${PEER_PORT}"
REE="start"

if [ "$4" = "start" ]; then
     PARA=$PARA" --initial-cluster="$(gen_member $QUORUM_SIZE $PEER_BASE_PORT $1)
     #PARA=$PARA" --join="$(gen_member $[ $MYID - 1 ] $CLIENT_BASE_PORT)
fi

echo "PARA: "$PARA
nohup ./pd-server --name="pd$MYID" --data-dir="pd"  --log-file=pd.log $PARA 2>&1 &