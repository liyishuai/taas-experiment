#!/usr/bin/env bash
/bin/cp ./Makefile ./client/
/bin/cp ./Makefile ./server/
if test -f /home/sengquan.zgh/workspace/TaaS/bin/pd-server; then
    /bin/cp /home/sengquan.zgh/workspace/TaaS/bin/pd-server ./server/pd-server
fi
if test -f /home/sengquan.zgh/workspace/TaaS/bin/pd-tso-bench; then
    /bin/cp /home/sengquan.zgh/workspace/TaaS/bin/pd-tso-bench ./client/pd-tso-bench
fi

tar -czvf taas.tar.gz --exclude="*.tar.gz" -C . ../taas
/home/sengquan.zgh/bin/oss_util put taas.tar.gz