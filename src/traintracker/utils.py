import arrow

def as_time(s):
    return arrow.get(s).to("Europe/Berlin")

def flatten_dict(di: dict, sep: str = "_"):
    def flatten(it, prefix="", sep="_"):
        for k, v in it:
            if isinstance(v, dict):
                yield from flatten(v.items(), f"{k}{sep}")
            else:
                if prefix:
                    k = prefix + str(k)
                yield (k, v)

    return dict(flatten(di.items()))


def fields_from_schema(schema, sep="_"):
    definitions = schema["definitions"]

    def flatten(typ, prefix=""):
        for k, v in typ["properties"].items():
            if "$ref" in v:
                typ_name = v["$ref"].removeprefix("#/definitions/")
                yield from flatten(definitions[typ_name], f"{k}{sep}")
            else:
                if prefix:
                    k = prefix + str(k)
                yield k

    return flatten(schema)


def placeholders(l, named=False):
    if named:
        if isinstance(l, dict):
            l = l.keys()
        return ",".join(f":{k}" for k in l)
    else:
        return ",".join(["?"] * len(l))
