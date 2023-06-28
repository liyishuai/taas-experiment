#### Build ####
CLIENT_NUM ?=
CURRENCY_NUM ?=
CONSENSUS_TYPE?=
TEST_TIME?=
LOG_PATH?=

IPLIST?=
QUROMSIZE?=
INDEX?=
RESTART?=
ROOT_PATH := $(shell pwd)
BUILD_BIN_PATH := $(ROOT_PATH)/bin


### Run tso benchmark locally
MYIP:=127.0.0.1
QUORUM_SIZE := 5
LOCAL_IP := $(shell hostname -i)

pd:
	@echo $(IPLIST)
	@echo $(QUROMSIZE)
	@echo $(INDEX)
	bash run.sh $(IPLIST) $(QUROMSIZE) $(INDEX) $(RESTART)

cl:
	cd playground; for  ((i=1;i<=$(QUORUM_SIZE); i++)); do rm -rf p$$i; done;
	for ((i=1;i<=$(QUORUM_SIZE); i++)); do pkill -of 'pd-server --name=pd$$i'; sleep 1; done;

taas:
	@echo $(CLIENT_NUM)
	@echo $(CURRENCY_NUM)
	@echo $(CONSENSUS_TYPE)
	@echo $(TEST_TIME)
	@echo $(LOCAL_IP)
	./pd-tso-bench -client $(CLIENT_NUM) -c $(CURRENCY_NUM) -duration $(TEST_TIME) -pd $(LOCAL_IP):3000  -v -dc $(CONSENSUS_TYPE) -ln 20000 > $(LOG_PATH)

.PHONY: pd taas cl 