# sqlization
Go library to turn jsonl into sqlite


Currently it turns jsonl into strings and json strings (if you present it a first-level [just under top] object)
Dynamically adds columns as you go, so if you hit it with a different value type expect them to show up in the same table
