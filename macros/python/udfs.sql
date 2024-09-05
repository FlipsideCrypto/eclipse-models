{% macro create_udf_ordered_signers(schema) %}
create or replace function {{ schema }}.udf_ordered_signers(accts array)
returns array
language python
runtime_version = '3.8'
handler = 'ordered_signers'
as
$$
def ordered_signers(accts) -> list:
    signers = [] 
    for v in accts:
        if v["signer"]:
            signers.append(v["pubkey"])

    return signers
$$;
{% endmacro %}

{% macro create_udf_get_compute_units_consumed(schema) %}
create or replace function {{ schema }}.udf_get_compute_units_consumed(log_messages array, instructions array)
returns int
language python 
runtime_version = '3.8'
handler = 'get_compute_units_consumed'
as 
$$
def get_compute_units_consumed(log_messages, instructions):
  import re
  if log_messages is None:
    return None
  units_consumed_list = []
  selected_logs = set()
  for instr in instructions:
    program_id = instr['programId']
    for logs in log_messages:
      if logs in selected_logs:
        continue
      if re.search(f"Program {program_id} consumed", logs):
        units_consumed = int(re.findall(r'consumed (\d+)', logs)[0])
        units_consumed_list.append(units_consumed)
        selected_logs.add(logs)
        break
  total_units_consumed = sum(units_consumed_list)
  return None if total_units_consumed == 0 else total_units_consumed
$$;
{% endmacro %}

{% macro create_udf_get_compute_units_total(schema) %}
create or replace function {{ schema }}.udf_get_compute_units_total(log_messages array, instructions array)
returns int
language python 
runtime_version = '3.8'
handler = 'get_compute_units_total'
as 
$$
def get_compute_units_total(log_messages, instructions):
  import re
  if log_messages is None:
    return None
  match = None
  for instr in instructions:
    program_id = instr['programId']
    for logs in log_messages:
      match = re.search(f"Program {program_id} consumed \d+ of (\d+) compute units", logs)
      if match:
        total_units = int(match.group(1))
        return total_units
  if match is None:
    return None
$$;
{% endmacro %}

{% macro create_udf_get_tx_size(schema) %}
create or replace function {{ schema }}.udf_get_tx_size(accts array, instructions array, version string, addr_lookups array, signers array)
returns int
language python
runtime_version = '3.8'
handler = 'get_tx_size' 
AS
$$
def get_tx_size(accts, instructions, version, addr_lookups, signers) -> int:

    header_size = 3
    n_signers = len(signers)
    n_pubkeys = len(accts)
    n_instructions = len(instructions)
    signature_size = (1 if n_signers <= 127 else (2 if n_signers <= 16383 else 3)) + (n_signers * 64)
    if version == '0':
        version_size = 1
        v0_non_lut_accts_size = len([acct for acct in accts if acct.get('source') == 'transaction'])
        account_pubkeys_size = (1 if n_pubkeys <= 127 else (2 if n_pubkeys <= 16383 else 3)) + (v0_non_lut_accts_size * 32)
    else:
        version_size = 0
        account_pubkeys_size = (1 if n_pubkeys <= 127 else (2 if n_pubkeys <= 16383 else 3)) + (n_pubkeys * 32)
    blockhash_size = 32
    program_id_index_size = (1 if n_instructions <= 127 else (2 if n_instructions <= 16383 else 3)) + (n_instructions)
    accounts_index_size = sum((1 if len(instruction.get('accounts', [])) <= 127 else (2 if len(instruction.get('accounts', [])) <= 16383 else 3)) + len(instruction.get('accounts', [])) for instruction in instructions)

    address_lookup_size = 0
    if version == '0' and addr_lookups:
        total_items = len(addr_lookups)
        readonly_items = sum(len(item.get('readonlyIndexes', [])) for item in addr_lookups)
        writeable_items = sum(len(item.get('writableIndexes', [])) for item in addr_lookups)
        address_lookup_size = (total_items * 34) + readonly_items + writeable_items
        address_lookup_size = (1 if address_lookup_size <= 127 else (2 if address_lookup_size <= 16383 else 3)) + address_lookup_size


    data_size = 0
    base58_chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    base58_map = {c: i for i, c in enumerate(base58_chars)}
    
    for instruction in instructions:
        bi = 0
        leading_zeros = 0
        data_base58 = instruction.get('data', b'')
        for c in data_base58:
            if c not in base58_map:
                raise ValueError('Invalid character in Base58 string')
            bi = bi * 58 + base58_map[c]

        hex_str = hex(bi)[2:]
        if len(hex_str) % 2 != 0:
            hex_str = '0' + hex_str

        for c in data_base58:
            if c == '1':
                leading_zeros += 2
            else:
                break
        

        temp_data_size = len('0' * leading_zeros + hex_str)
        data_size += (1 if temp_data_size / 2 <= 127 else (2 if temp_data_size / 2 <= 16383 else 3)) + (temp_data_size / 2)
    
    for instruction in instructions:
        if 'data' not in instruction:
            parsed = instruction.get('parsed')
            if isinstance(parsed, dict):
                type_ = parsed.get('type')
            else:
                type_ = None
        
            if type_ == 'transfer' and instruction.get('program') == 'spl-token':
                data_size += 7
                accounts_index_size += 4
            elif instruction.get('program') == 'spl-memo' and instruction.get('programId') == 'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr':
                data_size += 30
                accounts_index_size += 0
            elif type_ == 'transfer' and instruction.get('program') == 'system':
                data_size += 9
                accounts_index_size += 3
            elif instruction.get('program') == 'spl-memo' and instruction.get('programId') == 'Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo':
                data_size += 43
                accounts_index_size += 0
            elif type_ == 'transferChecked' and instruction.get('program') == 'spl-token':
                data_size += 8
                accounts_index_size += 5
            elif type_ == 'write' and instruction.get('program') == 'bpf-upgradeable-loader':
                info = parsed.get('info')
                if info:
                  bytes_data = info.get('bytes')
                  if bytes_data:
                    data_size += len(bytes_data) / 2
        
    final_data_size = data_size

    transaction_size = (
        header_size + account_pubkeys_size + blockhash_size +
        signature_size + program_id_index_size + accounts_index_size +
        final_data_size + address_lookup_size + version_size
    )

    return transaction_size

$$;
{% endmacro %}
