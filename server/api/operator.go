// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/schedule/operator"
	"github.com/tikv/pd/pkg/schedule/placement"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/pkg/utils/typeutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type operatorHandler struct {
	*server.Handler
	r *render.Render
}

func newOperatorHandler(handler *server.Handler, r *render.Render) *operatorHandler {
	return &operatorHandler{
		Handler: handler,
		r:       r,
	}
}

// @Tags     operator
// @Summary  Get a Region's pending operator.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {object}  schedule.OperatorWithStatus
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{region_id} [get]
func (h *operatorHandler) GetOperatorsByRegion(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["region_id"]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	op, err := h.GetOperatorStatus(regionID)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, op)
}

// @Tags     operator
// @Summary  List pending operators.
// @Param    kind  query  string  false  "Specify the operator kind."  Enums(admin, leader, region)
// @Produce  json
// @Success  200  {array}   operator.Operator
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [get]
func (h *operatorHandler) GetOperators(w http.ResponseWriter, r *http.Request) {
	var (
		results []*operator.Operator
		ops     []*operator.Operator
		err     error
	)

	kinds, ok := r.URL.Query()["kind"]
	if !ok {
		results, err = h.Handler.GetOperators()
		if err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	} else {
		for _, kind := range kinds {
			switch kind {
			case "admin":
				ops, err = h.GetAdminOperators()
			case "leader":
				ops, err = h.GetLeaderOperators()
			case "region":
				ops, err = h.GetRegionOperators()
			case "waiting":
				ops, err = h.GetWaitingOperators()
			}
			if err != nil {
				h.r.JSON(w, http.StatusInternalServerError, err.Error())
				return
			}
			results = append(results, ops...)
		}
	}

	h.r.JSON(w, http.StatusOK, results)
}

// FIXME: details of input json body params
// @Tags     operator
// @Summary  Create an operator.
// @Accept   json
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The operator is created."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators [post]
func (h *operatorHandler) CreateOperator(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.r, w, r.Body, &input); err != nil {
		return
	}

	name, ok := input["name"].(string)
	if !ok {
		h.r.JSON(w, http.StatusBadRequest, "missing operator name")
		return
	}

	switch name {
	case "transfer-leader":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["to_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing store id to transfer leader to")
			return
		}
		if err := h.AddTransferLeaderOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeIDs, ok := parseStoreIDsAndPeerRole(input["to_store_ids"], input["peer_roles"])
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store ids to transfer region to")
			return
		}
		if len(storeIDs) == 0 {
			h.r.JSON(w, http.StatusBadRequest, "missing store ids to transfer region to")
			return
		}
		if err := h.AddTransferRegionOperator(uint64(regionID), storeIDs); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "transfer-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		fromID, ok := input["from_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer from")
			return
		}
		toID, ok := input["to_store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddTransferPeerOperator(uint64(regionID), uint64(fromID), uint64(toID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "add-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddAddPeerOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "add-learner":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddAddLearnerOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "remove-peer":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		storeID, ok := input["store_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid store id to transfer peer to")
			return
		}
		if err := h.AddRemovePeerOperator(uint64(regionID), uint64(storeID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "merge-region":
		regionID, ok := input["source_region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		targetID, ok := input["target_region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "invalid target region id to merge to")
			return
		}
		if err := h.AddMergeRegionOperator(uint64(regionID), uint64(targetID)); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "split-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		policy, ok := input["policy"].(string)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing split policy")
			return
		}
		var keys []string
		if ks, ok := input["keys"]; ok {
			for _, k := range ks.([]interface{}) {
				key, ok := k.(string)
				if !ok {
					h.r.JSON(w, http.StatusBadRequest, "bad format keys")
					return
				}
				keys = append(keys, key)
			}
		}
		if err := h.AddSplitRegionOperator(uint64(regionID), policy, keys); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "scatter-region":
		regionID, ok := input["region_id"].(float64)
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "missing region id")
			return
		}
		group, _ := input["group"].(string)
		if err := h.AddScatterRegionOperator(uint64(regionID), group); err != nil {
			h.r.JSON(w, http.StatusInternalServerError, err.Error())
			return
		}
	case "scatter-regions":
		// support both receiving key ranges or regionIDs
		startKey, _ := input["start_key"].(string)
		endKey, _ := input["end_key"].(string)
		ids, ok := typeutil.JSONToUint64Slice(input["region_ids"])
		if !ok {
			h.r.JSON(w, http.StatusBadRequest, "region_ids is invalid")
			return
		}
		group, _ := input["group"].(string)
		// retry 5 times if retryLimit not defined
		retryLimit := 5
		if rl, ok := input["retry_limit"].(float64); ok {
			retryLimit = int(rl)
		}
		processedPercentage, err := h.AddScatterRegionsOperators(ids, startKey, endKey, group, retryLimit)
		errorMessage := ""
		if err != nil {
			errorMessage = err.Error()
		}
		s := struct {
			ProcessedPercentage int    `json:"processed-percentage"`
			Error               string `json:"error"`
		}{
			ProcessedPercentage: processedPercentage,
			Error:               errorMessage,
		}
		h.r.JSON(w, http.StatusOK, &s)
		return
	default:
		h.r.JSON(w, http.StatusBadRequest, "unknown operator")
		return
	}
	h.r.JSON(w, http.StatusOK, "The operator is created.")
}

// @Tags     operator
// @Summary  Cancel a Region's pending operator.
// @Param    region_id  path  int  true  "A Region's Id"
// @Produce  json
// @Success  200  {string}  string  "The pending operator is canceled."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/{region_id} [delete]
func (h *operatorHandler) DeleteOperatorByRegion(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["region_id"]

	regionID, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		h.r.JSON(w, http.StatusBadRequest, err.Error())
		return
	}

	if err = h.RemoveOperator(regionID); err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.r.JSON(w, http.StatusOK, "The pending operator is canceled.")
}

// @Tags     operator
// @Summary  lists the finished operators since the given timestamp in second.
// @Param    from  query  integer  false  "From Unix timestamp"
// @Produce  json
// @Success  200  {object}  []operator.OpRecord
// @Failure  400  {string}  string  "The request is invalid."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /operators/records [get]
func (h *operatorHandler) GetOperatorRecords(w http.ResponseWriter, r *http.Request) {
	var from time.Time
	if fromStr := r.URL.Query()["from"]; len(fromStr) > 0 {
		fromInt, err := strconv.ParseInt(fromStr[0], 10, 64)
		if err != nil {
			h.r.JSON(w, http.StatusBadRequest, err.Error())
			return
		}
		from = time.Unix(fromInt, 0)
	}
	records, err := h.GetRecords(from)
	if err != nil {
		h.r.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.r.JSON(w, http.StatusOK, records)
}

func parseStoreIDsAndPeerRole(ids interface{}, roles interface{}) (map[uint64]placement.PeerRoleType, bool) {
	items, ok := ids.([]interface{})
	if !ok {
		return nil, false
	}
	storeIDToPeerRole := make(map[uint64]placement.PeerRoleType)
	storeIDs := make([]uint64, 0, len(items))
	for _, item := range items {
		id, ok := item.(float64)
		if !ok {
			return nil, false
		}
		storeIDs = append(storeIDs, uint64(id))
		storeIDToPeerRole[uint64(id)] = ""
	}

	peerRoles, ok := roles.([]interface{})
	// only consider roles having the same length with ids as the valid case
	if ok && len(peerRoles) == len(storeIDs) {
		for i, v := range storeIDs {
			switch pr := peerRoles[i].(type) {
			case string:
				storeIDToPeerRole[v] = placement.PeerRoleType(pr)
			default:
			}
		}
	}
	return storeIDToPeerRole, true
}
