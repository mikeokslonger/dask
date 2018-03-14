
from dask.delayed_lambda import delayed
import dask.distributed
import random
import time


def f_prime(b):
    print 'running'
    time.sleep(random.randint(1, 3))
    return b ** 2


def f(a):
    return delayed(f_prime)(a)


if __name__ == '__main__':
    Client = dask.distributed.client_lambda.ClientLambda
    client = Client()  # set up local cluster on your laptop

    # print delayed(sum)(map(f, range(5))).compute()

    dag = delayed(f_prime)(5)
    print dag.compute()

    # time.sleep(60)
