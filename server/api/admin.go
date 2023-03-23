// Copyright 2018 TiKV Project Authors.
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
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/pkg/utils/apiutil"
	"github.com/tikv/pd/server"
	"github.com/unrolled/render"
)

type adminHandler struct {
	svr *server.Server
	rd  *render.Render
}

func newAdminHandler(svr *server.Server, rd *render.Render) *adminHandler {
	return &adminHandler{
		svr: svr,
		rd:  rd,
	}
}

// @Tags     admin
// @Summary  Drop a specific region from cache.
// @Param    id  path  integer  true  "Region Id"
// @Produce  json
// @Success  200  {string}  string  "The region is removed from server cache."
// @Failure  400  {string}  string  "The input is invalid."
// @Router   /admin/cache/region/{id} [delete]
func (h *adminHandler) DeleteRegionCache(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	vars := mux.Vars(r)
	regionIDStr := vars["id"]
	regionID, err := strconv.ParseUint(regionIDStr, 10, 64)
	if err != nil {
		h.rd.JSON(w, http.StatusBadRequest, err.Error())
		return
	}
	rc.DropCacheRegion(regionID)
	h.rd.JSON(w, http.StatusOK, "The region is removed from server cache.")
}

// @Tags     admin
// @Summary  Drop all regions from cache.
// @Produce  json
// @Success  200  {string}  string  "All regions are removed from server cache."
// @Router   /admin/cache/regions [delete]
func (h *adminHandler) DeleteAllRegionCache(w http.ResponseWriter, r *http.Request) {
	rc := getCluster(r)
	rc.DropCacheAllRegion()
	h.rd.JSON(w, http.StatusOK, "All regions are removed from server cache.")
}

// Intentionally no swagger mark as it is supposed to be only used in
// server-to-server. For security reason, it only accepts JSON formatted data.
func (h *adminHandler) SavePersistFile(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		h.rd.Text(w, http.StatusInternalServerError, "")
		return
	}
	defer r.Body.Close()
	if !json.Valid(data) {
		h.rd.Text(w, http.StatusBadRequest, "body should be json format")
		return
	}
	err = h.svr.PersistFile(mux.Vars(r)["file_name"], data)
	if err != nil {
		h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.rd.Text(w, http.StatusOK, "")
}

func (h *adminHandler) MarkSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	if err := h.svr.MarkSnapshotRecovering(); err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = h.rd.Text(w, http.StatusOK, "")
}

func (h *adminHandler) IsSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	marked, err := h.svr.IsSnapshotRecovering(r.Context())
	if err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	type resStruct struct {
		Marked bool `json:"marked"`
	}
	_ = h.rd.JSON(w, http.StatusOK, &resStruct{Marked: marked})
}

func (h *adminHandler) UnmarkSnapshotRecovering(w http.ResponseWriter, r *http.Request) {
	if err := h.svr.UnmarkSnapshotRecovering(r.Context()); err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = h.rd.Text(w, http.StatusOK, "")
}

// RecoverAllocID recover base alloc id
// body should be in {"id": "123"} format
func (h *adminHandler) RecoverAllocID(w http.ResponseWriter, r *http.Request) {
	var input map[string]interface{}
	if err := apiutil.ReadJSONRespondError(h.rd, w, r.Body, &input); err != nil {
		return
	}
	idValue, ok := input["id"].(string)
	if !ok || len(idValue) == 0 {
		_ = h.rd.Text(w, http.StatusBadRequest, "invalid id value")
		return
	}
	newID, err := strconv.ParseUint(idValue, 10, 64)
	if err != nil {
		_ = h.rd.Text(w, http.StatusBadRequest, err.Error())
		return
	}
	marked, err := h.svr.IsSnapshotRecovering(r.Context())
	if err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !marked {
		_ = h.rd.Text(w, http.StatusForbidden, "can only recover alloc id when recovering mark marked")
		return
	}

	leader := h.svr.GetLeader()
	if leader == nil {
		_ = h.rd.Text(w, http.StatusServiceUnavailable, errs.ErrLeaderNil.FastGenByArgs().Error())
		return
	}
	if err = h.svr.RecoverAllocID(r.Context(), newID); err != nil {
		_ = h.rd.Text(w, http.StatusInternalServerError, err.Error())
	}

	_ = h.rd.Text(w, http.StatusOK, "")
}
