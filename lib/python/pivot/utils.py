from __future__ import absolute_import


class dotdict(dict):
    """
    Provides dot.notation access to dictionary attributes
    """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def mutate_dict(inValue,
                keyFn=lambda k: k,
                valueFn=lambda v: v,
                keyTypes=None,
                valueTypes=None,
                **kwargs):
    """
    Takes an input dict or list-of-dicts and applies ``keyfn`` function to all of the keys in
    both the top-level and any nested dicts or lists, and ``valuefn`` to all
    If the input value is not of type `dict` or `list`, the value will be returned as-is.
    Args:
        inValue (any): The dict to mutate.
        keyFn (lambda): The function to apply to keys.
        valueFn (lambda): The function to apply to values.
        keyTypes (tuple, optional): If set, only keys of these types will be mutated
            with ``keyFn``.
        valueTypes (tuple, optional): If set, only values of these types will be mutated
            with ``valueFn``.
    Returns:
        A recursively mutated dict, list of dicts, or the value as-is (described above).
    """

    # this is here as a way of making sure that the various places where recursion is done always
    # performs the same call, preserving all arguments except for value (which is what changes
    # between nested calls).
    def recurse(value):
        return mutate_dict(value,
                           keyFn=keyFn,
                           valueFn=valueFn,
                           keyTypes=keyTypes,
                           valueTypes=valueTypes,
                           **kwargs)

    # handle dicts
    if isinstance(inValue, dict):
        # create the output dict
        outputDict = dict()

        # for each dict item...
        for k, v in inValue.items():
            # apply the keyFn to some or all of the keys we encounter
            if keyTypes is None or (isinstance(keyTypes, tuple) and isinstance(k, keyTypes)):
                # prepare the new key
                k = keyFn(k, **kwargs)

            # apply the valueFn to some or all of the values we encounter
            if valueTypes is None or (isinstance(valueTypes, tuple) and isinstance(v, valueTypes)):
                v = valueFn(v)

            # recurse depending on the value's type
            #
            if isinstance(v, dict):
                # recursively call mutate_dict() for nested dicts
                outputDict[k] = recurse(v)
            elif isinstance(v, list):
                # recursively call mutate_dict() for each element in a list
                outputDict[k] = [recurse(i) for i in v]
            else:
                # set the value straight up
                outputDict[k] = v

        # return the now-populated output dict
        return outputDict

    # handle lists-of-dicts
    elif isinstance(inValue, list) and len(inValue) > 0:
        return [recurse(i) for i in inValue]

    else:
        # passthrough non-dict value as-is
        return inValue


def compact(inDict, keep_if=lambda k, v: v is not None):
    """
    Takes a dictionary and returns a copy with elements matching a given lambda removed. The
    default behavior will remove any values that are `None`.
    Args:
        inDict (dict): The dictionary to operate on.
        keep_if (lambda(k,v), optional): A function or lambda that will be called for each
            (key, value) pair.  If the function returns truthy, the element will be left alone,
            otherwise it will be removed.
    """
    if isinstance(inDict, dict):
        return {
            k: v for k, v in inDict.items() if keep_if(k, v)
        }

    raise ValueError("Expected: dict, got: {0}".format(type(inDict)))
