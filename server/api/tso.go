// Copyright 2021 TiKV Project Authors.
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
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type tsoHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newTSOHandler(svr *server.Server, rd *render.Render) *tsoHandler {
	return &tsoHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     tso
// @Summary  Transfer Local TSO Allocator
// @Accept   json
// @Param    name  path  string  true  "PD server name"
// @Param    body  body  object  true  "json params"
// @Produce  json
// @Success  200  {string}  string  "The transfer command is submitted."
// @Failure  400  {string}  string  "The input is invalid."
// @Failure  404  {string}  string  "The member does not exist."
// @Failure  500  {string}  string  "PD server failed to proceed the request."
// @Router   /tso/allocator/transfer/{name} [post]
func (h *tsoHandler) TransferLocalTSOAllocator(w http.ResponseWriter, r *http.Request) {
	members, membersErr := getMembers(h.svr)
	if membersErr != nil {
		h.rd.JSON(w, http.StatusInternalServerError, membersErr.Error())
		return
	}
	name := mux.Vars(r)["name"]
	dcLocation := r.URL.Query().Get("dcLocation")
	if len(dcLocation) < 1 {
		h.rd.JSON(w, http.StatusBadRequest, "dcLocation is undefined")
		return
	}
	var memberID uint64
	for _, m := range members.GetMembers() {
		if m.GetName() == name {
			memberID = m.GetMemberId()
			break
		}
	}
	if memberID == 0 {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("not found, pd: %s", name))
		return
	}
	// TODO: support local tso forward in api service mode in the future.
	err := h.svr.GetTSOAllocatorManager().TransferAllocatorForDCLocation(dcLocation, memberID)
	if err != nil {
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.JSON(w, http.StatusOK, "The transfer command is submitted.")
}
