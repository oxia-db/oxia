// Copyright 2023-2025 The Oxia Authors
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

package metadata

import (
	"errors"
	"strconv"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/testing"
)

func K8SResourceVersionSupport(tracker testing.ObjectTracker) testing.ReactionFunc {
	return func(action testing.Action) (handled bool, ret runtime.Object, err error) {
		ns := action.GetNamespace()
		gvr := action.GetResource()
		switch action := action.(type) {
		case testing.CreateActionImpl:
			objMeta := accessor(action.GetObject())
			objMeta.SetResourceVersion("0")
			return false, action.GetObject(), nil
		case testing.UpdateActionImpl:
			objMeta := accessor(action.GetObject())
			existing, err := tracker.Get(gvr, ns, objMeta.GetName())
			if err != nil {
				//nolint:nilerr
				return false, action.GetObject(), nil
			}
			existingObjMeta := accessor(existing)
			if objMeta.GetResourceVersion() != existingObjMeta.GetResourceVersion() {
				return true, action.GetObject(), k8serrors.NewConflict(gvr.GroupResource(), objMeta.GetName(), errors.New("conflict"))
			}
			incrementVersion(objMeta)
			return false, action.GetObject(), nil
		}

		return false, nil, nil
	}
}

func accessor(obj runtime.Object) metav1.Object {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		panic(err)
	}
	return objMeta
}

func incrementVersion(metaObj metav1.Object) {
	i, err := strconv.ParseUint(metaObj.GetResourceVersion(), 10, 64)
	if err != nil {
		panic(err)
	}
	i++
	str := strconv.FormatUint(i, 10)
	metaObj.SetResourceVersion(str)
}
