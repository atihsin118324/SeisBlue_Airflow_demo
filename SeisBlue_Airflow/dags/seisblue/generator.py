import h5py
import torch
from torch.utils.data import TensorDataset, DataLoader, Dataset, random_split
from torch.utils.data.sampler import SubsetRandomSampler, SequentialSampler
import numpy as np
import random

from seisblue import tool


class H5Dataset(Dataset):
    def __init__(self, h5_paths, limit=-1):
        self.limit = limit
        self.h5_paths = h5_paths
        self._archives = [h5py.File(h5_path, "r") for h5_path in self.h5_paths]
        self.indices = {}
        idx = 0
        for a, archive in enumerate(self.archives):
            for i in range(len(archive)):
                self.indices[idx] = (a, i)
                idx += 1

        self._archives = None

    @property
    def archives(self):
        if self._archives is None: # lazy loading here!
            self._archives = [h5py.File(h5_path, "r") for h5_path in self.h5_paths]
        return self._archives

    def __getitem__(self, index):
        a, i = self.indices[index]
        archive = self.archives[a]
        for instance in archive.values():
            instance = tool.read_hdf5(instance)
            features = torch.Tensor(np.array([trace.data for trace in instance.traces]))
            labels = torch.Tensor(instance.labels[0].data.T)
        return features, labels

    def __len__(self):
        if self.limit > 0:
            return min([len(self.indices), self.limit])
        return len(self.indices)


class PickDataset(Dataset):
    def __init__(self, instances):
        self.features = torch.zeros(len(instances), 3, instances[0].timewindow.npts)
        self.labels = torch.zeros_like(self.features)
        for i, instance in enumerate(instances):
            for j, trace in enumerate(instance.traces):
                self.features[i, j, :] = torch.Tensor(trace.data)
            self.labels[i, :, :] = torch.tensor(instance.labels[0].data.T)

    def __getitem__(self, index):
        feature = self.features[index]
        label = self.labels[index]
        return feature, label

    def __len__(self):
        return len(self.features)


def get_instance(input_filepath):
    instances = []
    with h5py.File(input_filepath, "r") as f:
        for instance in f.values():
            instance = tool.read_hdf5(instance)
            instances.append(instance)
    return instances


def get_loaders(mode, dataset, batch_size=32, train_ratio=0.7):
    assert dataset[0][0].size() == dataset[0][1].size(), \
        f"feature and label don't have the same size.\n" \
        f"feature:{dataset[0][0].size()}, label{dataset[0][1].size()}"

    if mode == 'train':
        dataset_size = len(dataset)
        indices = list(range(dataset_size))
        split = int(np.floor(train_ratio * dataset_size))

        train_indices, val_indices = indices[split:], indices[:split]
        train_sampler = SubsetRandomSampler(train_indices)
        valid_sampler = SubsetRandomSampler(val_indices)

        train_loader = DataLoader(dataset,
                                batch_size=batch_size,
                                sampler=train_sampler,
                                shuffle=False,)

        valid_loader = DataLoader(dataset,
                                batch_size=batch_size,
                                sampler=valid_sampler,
                                shuffle=False,)

        return train_loader, valid_loader

    elif mode == 'test':
        test_loader = DataLoader(dataset,
                                 batch_size=batch_size,
                                 shuffle=False, )
        return test_loader


if __name__ == '__main__':
    pass