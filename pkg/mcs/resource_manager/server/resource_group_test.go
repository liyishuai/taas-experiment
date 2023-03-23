package server

import (
	"encoding/json"
	"testing"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestPatchResourceGroup(t *testing.T) {
	re := require.New(t)
	rg := &ResourceGroup{Name: "test", Mode: rmpb.GroupMode_RUMode, RUSettings: NewRequestUnitSettings(nil)}
	testCaseRU := []struct {
		patchJSONString  string
		expectJSONString string
	}{
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000},"state":{"initialized":false}}},"priority":0}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}}`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}},"priority":0}`},
		{`{"name":"test", "mode":1, "r_u_settings": {"r_u":{"settings":{"fill_rate": 200000, "burst_limit": -1}}}, "priority": 8 }`,
			`{"name":"test","mode":1,"r_u_settings":{"r_u":{"settings":{"fill_rate":200000,"burst_limit":-1},"state":{"initialized":false}}},"priority":8}`},
	}

	for _, ca := range testCaseRU {
		patch := &rmpb.ResourceGroup{}
		err := json.Unmarshal([]byte(ca.patchJSONString), patch)
		re.NoError(err)
		err = rg.PatchSettings(patch)
		re.NoError(err)
		res, err := json.Marshal(rg.Copy())
		re.NoError(err)
		re.Equal(ca.expectJSONString, string(res))
	}
}
