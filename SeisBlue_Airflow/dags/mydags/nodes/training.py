import argparse
import torch
from tqdm import tqdm
from torch.utils.data import TensorDataset, DataLoader

from seisblue import tool, plot, generator, SQL
from seisblue.model import PhaseNet


def get_instance_filepaths(database, **kwargs):
    client = SQL.Client(database)
    waveforms = client.get_waveform(**kwargs)

    filepaths = []
    # input_filepath = h5py.File('/tmp/dataset/train.hdf5', 'a')
    # input_filepath.create_group('filepaths')
    for waveform in waveforms:
        filepath = waveform.datasetpath
        # input_filepath['ext link'] = h5py.ExternalLink(filepath, '/filepaths')
        filepaths.append(filepath)
    print(f'Get {len(filepaths)} filepaths for training.')
    return filepaths


def loss_fn(y_pred, y_true, eps=1e-5):
    h = y_true * torch.log(y_pred + eps)
    h = h.mean(-1).sum(-1)
    h = h.mean()
    return -h


def train_loop(model, dataloader, optimizer):
    size = len(dataloader.dataset)
    for batch_id, (batch_X, batch_y) in tqdm(enumerate(dataloader)):
        pred = model(batch_X)
        loss = loss_fn(pred, batch_y)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch_id % 5 == 0:
            loss, current = loss.item(), batch_id * batch_X.shape[0]
            tqdm.write(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def valid_loop(model, dataloader):
    num_batches = len(dataloader)
    valid_loss = 0

    with torch.no_grad():
        for (batch_X, batch_y) in tqdm(dataloader):
            pred = model(batch_X)
            valid_loss += loss_fn(pred, batch_y).item()

    valid_loss /= num_batches
    tqdm.write(f"Test avg loss: {valid_loss:>8f} \n")


def train_valid(model, model_path, train_loader, val_loader, epochs, learning_rate):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f'Use {device} device.')
    optimizer = torch.optim.Adam(model.parameters(), lr=eval(learning_rate))

    for t in range(epochs):
        print(f"Epoch {t + 1}\n-------------------------------")
        train_loop(model, train_loader, optimizer)
        valid_loop(model, val_loader)

    torch.save({
        'epoch': epochs,
        'model_state_dict': model.state_dict(),
        'optimizer_state_dict': optimizer.state_dict(),
    }, model_path)
    print(f"Save model in {model_path}.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_filepath", type=str, required=True)
    args = parser.parse_args()

    waveform_filter = {'from_time': '2017-04-01T00:00:00', 'to_time': '2017-04-30T12:00:00'}
    model_config = tool.read_yaml(args.config_filepath)
    model_filepath = model_config['global']['model_filepath']
    database = model_config['global']['database']
    c = model_config['train']
    loader_parameters = c['loader_parameters']
    train_valid_parameters = c['train_valid_parameters']

    print("Preparing dataset.")
    filepaths = get_instance_filepaths(database, **waveform_filter)
    # if c['plot']:
    #     plot.plot_dataset(instances[0], save_dir='/tmp/dataset/')
    train_loader = torch.utils.data.DataLoader(generator.H5Dataset(filepaths[:-2]), batch_size=32, num_workers=2)
    val_loader = torch.utils.data.DataLoader(generator.H5Dataset(filepaths[-2:]), batch_size=32, num_workers=2)
    # batch = next(iter(train_loader))
    # print(batch[0].shape, batch[1].shape)
    print("Start to tran and valid.")
    train_valid(PhaseNet(), model_filepath, train_loader, val_loader, **train_valid_parameters)
