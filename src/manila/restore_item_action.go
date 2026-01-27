package manila

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	kindKey             = "kind"
	persistentVolumeKey = "PersistentVolume"
	manilaCSIDriver     = "manila.csi.openstack.org"
)

type RestoreItemAction struct {
	log logrus.FieldLogger
}

func NewRestoreItemAction(logger logrus.FieldLogger) *RestoreItemAction {
	return &RestoreItemAction{log: logger}
}

// AppliesTo returns information about which resources this action should be invoked for.
// A RestoreItemAction's Execute function will only be invoked on items that match the returned
// selector. A zero-valued ResourceSelector matches all resources.
func (p *RestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumes"},
	}, nil
}

// Execute allows the RestorePlugin to perform arbitrary logic with the item being restored,
// in this case, handling Released PVs by removing finalizers.
func (p *RestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	p.log.Info("Manila RestoreItemAction: processing PersistentVolume")

	inputMap := input.Item.UnstructuredContent()

	kind, ok := inputMap[kindKey].(string)
	if !ok {
		return nil, errors.New("failed to get kind from input item")
	}

	if kind != persistentVolumeKey {
		return &velero.RestoreItemActionExecuteOutput{
			UpdatedItem: input.Item,
		}, nil
	}

	pv := new(corev1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(inputMap, pv); err != nil {
		return nil, errors.Wrap(err, "failed to convert unstructured item to PersistentVolume")
	}

	pvName := pv.Name
	pvPhase := pv.Status.Phase

	p.log.WithFields(logrus.Fields{
		"pvName":  pvName,
		"phase":   pvPhase,
		"driver":  getPVDriver(pv),
	}).Info("examining PersistentVolume")

	if !isManilaCSIPV(pv) {
		p.log.Infof("PV %s is not Manila CSI, skipping", pvName)
		return &velero.RestoreItemActionExecuteOutput{
			UpdatedItem: input.Item,
		}, nil
	}

	if pvPhase == corev1.VolumeReleased {
		p.log.Infof("PV %s is in Released state, removing finalizers", pvName)
		
		originalFinalizers := pv.Finalizers
		pv.Finalizers = []string{}
		
		p.log.WithFields(logrus.Fields{
			"pvName":             pvName,
			"originalFinalizers": originalFinalizers,
			"newFinalizers":      pv.Finalizers,
		}).Info("removed finalizers from Released PV")
		
		if pv.Spec.ClaimRef != nil {
			p.log.Infof("PV %s has claimRef, considering removal", pvName)
		}
	} else {
		p.log.WithFields(logrus.Fields{
			"pvName": pvName,
			"phase":  pvPhase,
		}).Info("PV is not in Released state, no modification needed")
	}

	updatedMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert PersistentVolume to unstructured")
	}

	return velero.NewRestoreItemActionExecuteOutput(&unstructured.Unstructured{Object: updatedMap}), nil
}

func isManilaCSIPV(pv *corev1.PersistentVolume) bool {
	return pv.Spec.CSI != nil && pv.Spec.CSI.Driver == manilaCSIDriver
}

func getPVDriver(pv *corev1.PersistentVolume) string {
	if pv.Spec.CSI != nil {
		return pv.Spec.CSI.Driver
	}
	return "non-csi"
}