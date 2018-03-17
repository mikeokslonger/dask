from __future__ import absolute_import, division, print_function

from collections import Iterator
import operator
import uuid

try:
    from cytoolz import curry, pluck
except ImportError:
    from toolz import curry, pluck

from . import threaded_lambda
from .base import Base, is_dask_collection, dont_optimize
from .base import tokenize as _tokenize
from .compatibility import apply
from .core import quote
from .context import _globals, globalmethod
from .utils import funcname, methodcaller, OperatorMethodMixin
from . import sharedict

__all__ = ['Delayed', 'delayed']


def unzip(ls, nout):
    out = list(zip(*ls))
    if not out:
        out = [()] * nout
    return out


def to_task_dask(expr):
    if isinstance(expr, Delayed):
        return {'s3_bucket': 'mikeokslonger-dask', 's3_key': expr.key, 'read_me': True}, expr.dask

    if is_dask_collection(expr):
        name = 'finalize-' + tokenize(expr, pure=True)
        keys = expr.__dask_keys__()
        opt = getattr(expr, '__dask_optimize__', dont_optimize)
        finalize, args = expr.__dask_postcompute__()
        dsk = {name: (finalize, keys) + args}
        dsk.update(opt(expr.__dask_graph__(), keys))
        return name, dsk

    if isinstance(expr, Iterator):
        expr = list(expr)
    typ = type(expr)

    if typ in (list, tuple, set):
        args, dasks = unzip((to_task_dask(e) for e in expr), 2)
        args = list(args)
        dsk = sharedict.merge(*dasks)
        # Ensure output type matches input type
        return (args, dsk) if typ is list else ((typ, args), dsk)

    if typ is dict:
        args, dsk = to_task_dask([[k, v] for k, v in expr.items()])
        return (dict, args), dsk

    if typ is slice:
        args, dsk = to_task_dask([expr.start, expr.stop, expr.step])
        return (slice,) + tuple(args), dsk

    return expr, {}


def tokenize(*args, **kwargs):
    pure = kwargs.pop('pure', None)
    if pure is None:
        pure = _globals.get('delayed_pure', False)

    if pure:
        return _tokenize(*args, **kwargs)
    else:
        return str(uuid.uuid4())


@curry
def delayed(obj, name=None, pure=None, nout=None, traverse=True):
    if isinstance(obj, Delayed):
        return obj

    if is_dask_collection(obj) or traverse:
        task, dsk = to_task_dask(obj)
    else:
        task = quote(obj)
        dsk = {}

    if task is obj:
        if not (nout is None or (type(nout) is int and nout >= 0)):
            raise ValueError("nout must be None or a non-negative integer,"
                             " got %s" % nout)
        if not name:
            try:
                prefix = obj.__name__
            except AttributeError:
                prefix = type(obj).__name__
            token = tokenize(obj, nout, pure=pure)
            name = '%s-%s' % (prefix, token)
        return DelayedLeaf(obj, name, pure=pure, nout=nout)
    else:
        if not name:
            name = '%s-%s' % (type(obj).__name__, tokenize(task, pure=pure))
        dsk = sharedict.merge(dsk, (name, {name: task}))
        return Delayed(name, dsk)


def right(method):
    """Wrapper to create 'right' version of operator given left version"""
    def _inner(self, other):
        return method(other, self)
    return _inner


def rebuild(dsk, key, length):
    return Delayed(key, dsk, length)


class Delayed(Base, OperatorMethodMixin):
    __slots__ = ('_key', 'dask', '_length')

    def __init__(self, key, dsk, length=None):
        self._key = key
        if type(dsk) is list:  # compatibility with older versions
            dsk = sharedict.merge(*dsk)
        self.dask = dsk
        self._length = length

    def __dask_graph__(self):
        return self.dask

    def __dask_keys__(self):
        return [self.key]

    def __dask_tokenize__(self):
        return self.key

    __dask_scheduler__ = staticmethod(threaded_lambda.get)
    __dask_optimize__ = globalmethod(dont_optimize, key='delayed_optimize')

    def __dask_postcompute__(self):
        return single_key, ()

    def __dask_postpersist__(self):
        return rebuild, (self._key, getattr(self, '_length', None))

    def __getstate__(self):
        return tuple(getattr(self, i) for i in self.__slots__)

    def __setstate__(self, state):
        for k, v in zip(self.__slots__, state):
            setattr(self, k, v)

    @property
    def key(self):
        return self._key

    def __repr__(self):
        return "Delayed({0})".format(repr(self.key))

    def __hash__(self):
        return hash(self.key)

    def __dir__(self):
        return dir(type(self))

    def __getattr__(self, attr):
        if attr.startswith('_'):
            raise AttributeError("Attribute {0} not found".format(attr))
        return DelayedAttr(self, attr)

    def __setattr__(self, attr, val):
        if attr in self.__slots__:
            object.__setattr__(self, attr, val)
        else:
            raise TypeError("Delayed objects are immutable")

    def __setitem__(self, index, val):
        raise TypeError("Delayed objects are immutable")

    def __iter__(self):
        if getattr(self, '_length', None) is None:
            raise TypeError("Delayed objects of unspecified length are "
                            "not iterable")
        for i in range(self._length):
            yield self[i]

    def __len__(self):
        if getattr(self, '_length', None) is None:
            raise TypeError("Delayed objects of unspecified length have "
                            "no len()")
        return self._length

    def __call__(self, *args, **kwargs):
        pure = kwargs.pop('pure', None)
        name = kwargs.pop('dask_key_name', None)
        func = delayed(apply, pure=pure)
        if name is not None:
            return func(self, args, kwargs, dask_key_name=name)
        return func(self, args, kwargs)

    def __bool__(self):
        raise TypeError("Truth of Delayed objects is not supported")

    __nonzero__ = __bool__

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        method = delayed(right(op) if inv else op, pure=True)
        return lambda *args, **kwargs: method(*args, **kwargs)

    _get_unary_operator = _get_binary_operator


def call_function(func, func_token, args, kwargs, pure=None, nout=None):
    dask_key_name = kwargs.pop('dask_key_name', None)
    pure = kwargs.pop('pure', pure)

    if dask_key_name is None:
        name = '%s-%s' % (funcname(func),
                          tokenize(func_token, *args, pure=pure, **kwargs))
    else:
        name = dask_key_name

    dsk = sharedict.ShareDict()
    args_dasks = list(map(to_task_dask, args))
    for arg, d in args_dasks:
        if isinstance(d, sharedict.ShareDict):
            dsk.update_with_key(d)
        elif isinstance(arg, (str, tuple)):
            dsk.update_with_key(d, key=arg)
        else:
            dsk.update(d)

    args = tuple(pluck(0, args_dasks))

    if kwargs:
        dask_kwargs, dsk2 = to_task_dask(kwargs)
        dsk.update(dsk2)
        task = (apply, func, list(args), dask_kwargs)
    else:
        task = (func,) + args

    dsk.update_with_key({name: task}, key=name)
    nout = nout if nout is not None else None
    return Delayed(name, dsk, length=nout)


class DelayedLeaf(Delayed):
    __slots__ = ('_obj', '_key', '_pure', '_nout')

    def __init__(self, obj, key, pure=None, nout=None):
        self._obj = obj
        self._key = key
        self._pure = pure
        self._nout = nout

    @property
    def dask(self):
        return {self._key: self._obj}

    def __call__(self, *args, **kwargs):
        return call_function(self._obj, self._key, args, kwargs,
                             pure=self._pure, nout=self._nout)


class DelayedAttr(Delayed):
    __slots__ = ('_obj', '_attr', '_key')

    def __init__(self, obj, attr):
        self._obj = obj
        self._attr = attr
        self._key = 'getattr-%s' % tokenize(obj, attr, pure=True)

    @property
    def dask(self):
        dsk = {self._key: (getattr, self._obj._key, self._attr)}
        return sharedict.merge(self._obj.dask, (self._key, dsk))

    def __call__(self, *args, **kwargs):
        return call_function(methodcaller(self._attr), self._attr, (self._obj,) + args, kwargs)


for op in [operator.abs, operator.neg, operator.pos, operator.invert,
           operator.add, operator.sub, operator.mul, operator.floordiv,
           operator.truediv, operator.mod, operator.pow, operator.and_,
           operator.or_, operator.xor, operator.lshift, operator.rshift,
           operator.eq, operator.ge, operator.gt, operator.ne, operator.le,
           operator.lt, operator.getitem]:
    Delayed._bind_operator(op)


def single_key(seq):
    """ Pick out the only element of this list, a list of keys """
    return seq[0]
