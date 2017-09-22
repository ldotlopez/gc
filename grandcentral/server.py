from grandcentral import (
    api,
    storage
)

app = api.API(storage.MemoryStorage())
