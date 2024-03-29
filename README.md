# Ansible Collection - `anophelesgreyhoe.solarwinds`

## Collection Dependencies

- [orionsdk](https://pypi.org/project/orionsdk/) Python API for Solarwinds Orion
  SDK

## Included modules

| Name              | Description                              |
| ----------------- | ---------------------------------------- |
| `orion_node`      | Manage nodes in SolarWinds Orion         |
| `solarwinds_info` | Query the SolarWinds Information Service |

See module documentation for more information on usage.

## Examples

Rather than simple playbooks as below, you may wish to create your own
collections/roles for greater flexibility, and to allow these to be called from
other playbooks.

### `orion_node`

#### Add an SNMP or WMI node

NB: to add a WMI node, simply change `polling_method` to `wmi` and ensure that
`credential_names` includes names of one or more WMI credentials with access.

```yaml
- name: Discover and add node to Orion if not already
  hosts: all
  gather_facts: false
  vars:
    credential_names:
      - snmp_credential1
      - snmp_credential2
    custom_properties:
      Country: New Zealand
      Site: Head Office
    # For SNMP and WMI, interface filtering can be performed at discovery time
    discovery_interface_filters:
      - Prop: "Name"
        Op: "!Regex"
        Val: "^(br-|docker|lo|veth).*"
    volume_filters:
      - type: "RAM"
      - type: "Virtual Memory"
      - name: "Memory buffers"
      - name: "Cached memory"
      - name: "Shared memory"
      - name: "/dev/shm"
      - name: "/run"
      - name: "/sys/fs/cgroup"
      - name: "/var/lib/docker/containers/"
  tasks:
    - anophelesgreyhoe.solarwinds.orion_node:
        solarwinds_connection:
          hostname: "{{ solarwinds_hostname }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        node_name: "{{ inventory_hostname }}"
        state: present
        polling_method: snmp
        credential_names: "{{ credential_names }}"
        discovery_interface_filters: "{{ discovery_interface_filters }}"
        volume_filters: "{{ volume_filters }}"
        custom_properties: "{{ custom_properties }}"
      delegate_to: localhost
      throttle: 1
```

#### Add a passive agent (server-initiated communication) node

Again, the below example is simplified. A more elaborate playbook might:

1. Randomly generate an ephemeral shared secret.

1. Deploy the SolarWinds agent software to the target, secured with that secret.

1. Configure the target's local firewall to allow the polling engine's access.

1. Add the agent to SolarWinds, providing the same shared secret to SolarWinds.

```yaml
- name: Discover and add passive agent node to Orion if not already
  hosts: all
  gather_facts: false
  vars:
    custom_properties:
      Country: New Zealand
      Site: Head Office
    # Interface filters remove resources after they have been added
    interface_filters:
      - type: "Loopback"
    volume_filters:
      - type: "RAM"
      - type: "Virtual Memory"
      - name: "Memory buffers"
      - name: "Cached memory"
      - name: "Shared memory"
      - name: "/dev/shm"
      - name: "/run"
      - name: "/sys/fs/cgroup"
      - name: "/var/lib/docker/containers/"
  tasks:
    - anophelesgreyhoe.solarwinds.orion_node:
        solarwinds_connection:
          hostname: "{{ solarwinds_hostname }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        node_name: "{{ inventory_hostname }}"
        state: present
        polling_method: agent
        agent_mode: passive
        agent_port: 17790
        agent_shared_secret: "{{ shared_secret }}"
        interface_filters: "{{ interface_filters }}"
        volume_filters: "{{ volume_filters }}"
        custom_properties: "{{ custom_properties }}"
      delegate_to: localhost
      throttle: 1
```

### `solarwinds_info`

#### Get list of nodes in Australia with 10.150.* IP addresses

Note that we're returning only selected properties for the node and related
entities.

```yaml
- name: Node info
  hosts: localhost
  gather_facts: false
  tasks:
    - name: List Australia nodes with 10.150.* IP addresses
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_hostname }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table:
          name: Orion.Nodes
          columns:
            - NodeID
            - Caption
            - IP
            - StatusDescription
        nested_entities:
          CustomProperties:
            columns:
              - Country
        filters:
          - include:
              IPAddress: "10.150.%"
              CustomProperties.Country: Australia
      delegate_to: localhost
      throttle: 1
      register: info
```
