package faas

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/serverlessresearch/srk/pkg/srkmgr"
)

func InvokeFaasSort(mgr *srkmgr.SrkManager, arg *FaasArg) error {
	jsonArg, err := json.Marshal(arg)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal FaaS argument")
	}

	rawResp, err := mgr.Provider.Faas.Invoke("radixsort", string(jsonArg))
	if err != nil {
		return fmt.Errorf("Failed to invoke function: %v\n", err)
	}

	respBytes := rawResp.Bytes()

	var resp FaasResp
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return errors.Wrapf(err, "Couldn't parse function response: %v", string(respBytes))
	}

	if !resp.Success {
		return fmt.Errorf("Remote function error: %v", resp.Err)
	}

	return nil
}
