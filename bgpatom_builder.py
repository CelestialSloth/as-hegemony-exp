from collections import defaultdict
from confluent_kafka import Consumer
from particles_handler import ParticlesHandler

import logging
import stream
import utils


def remove_path_prepending(prepended_as_path: list):
    prev = ""
    as_path = list()
    for asn in prepended_as_path:
        if asn == prev:
            continue
        as_path.append(asn)
        prev = asn
    return as_path


def atom_aspath_encoding(as_path: str):
    non_prepended_as_path = remove_path_prepending(as_path.split(" "))
    return tuple(non_prepended_as_path[:-1])


class BGPAtomBuilder:
    def __init__(self):
        self.prefix_to_atom_id = defaultdict(list)
        self.particles_handler = ParticlesHandler()

    def resize_prefix_atom_id_length(self, prefix):
        target_size = self.particles_handler.number_of_particles
        current_size = len(self.prefix_to_atom_id[prefix])
        self.prefix_to_atom_id[prefix] += ["*"] * (target_size-current_size)

    def read_ribs_and_add_particles_to_atom(self, consumer: Consumer, timestamp: int):
        for element in stream.consume_rib_message_at(consumer, timestamp):
            peer_address = element["peer_address"]
            as_path = element["fields"]["as-path"]
            prefix = element["fields"]["prefix"]

            if prefix == "0.0.0.0/0":
                continue

            particle = self.particles_handler.get_particle_by_peer_address(peer_address)
            particle.increment_prefixes_count()

            atom_as_path = atom_aspath_encoding(as_path)
            particle_id = particle.idx
            aspath_id = particle.get_id_by_aspath(atom_as_path)

            self.resize_prefix_atom_id_length(prefix)
            self.prefix_to_atom_id[prefix][particle_id] = str(aspath_id)

    def remove_none_full_fleet_particles(self, threshold: int):
        none_full_fleet = self.particles_handler.get_none_full_fleet_particles(threshold)

        for prefix in self.prefix_to_atom_id:
            self.resize_prefix_atom_id_length(prefix)
            for none_full_fleet_particle_id in none_full_fleet:
                self.prefix_to_atom_id[prefix][none_full_fleet_particle_id] = "-"

    def dump_bgpatom(self):
        bgpatom_id_to_prefixes = defaultdict(list)
        for prefix in self.prefix_to_atom_id:
            atom_id = "|".join(self.prefix_to_atom_id[prefix])
            bgpatom_id_to_prefixes[atom_id].append(prefix)
        return bgpatom_id_to_prefixes


if __name__ == "__main__":
    BGP_DATA_TOPIC = "ihr_bgp_rrc10_ribs"
    BGPATOM_FULL_FLEET_THRESHOLD = 600000

    offset_time_string = "2020-08-01T00:00:00"
    offset_datetime = utils.str2dt(offset_time_string, utils.DATETIME_STRING_FORMAT)
    offset_timestamp = utils.dt2ts(offset_datetime)

    ribs_consumer = stream.create_consumer_and_set_offset(BGP_DATA_TOPIC, offset_timestamp)
    bgpatom_builder = BGPAtomBuilder()
    bgpatom_builder.read_ribs_and_add_particles_to_atom(ribs_consumer, offset_timestamp)
    bgpatom_builder.remove_none_full_fleet_particles(BGPATOM_FULL_FLEET_THRESHOLD)

    bgpatom = bgpatom_builder.dump_bgpatom()
    print("topic: ", BGP_DATA_TOPIC)
    print("number of atom: ", len(bgpatom))