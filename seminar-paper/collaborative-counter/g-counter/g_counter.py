import asyncio
from pycrdt import Doc, Map


def get_total_count(y_map: Map) -> int:
    total = 0
    for key in y_map.keys():
        total += y_map.get(key) or 0
    return total


async def run_g_counter_example():
    print("👻----Starting G-Counter simulation----👻")

    # 3 independent replicas (Docs)
    doc1 = Doc(allow_multithreading=False)  # Matija
    doc2 = Doc(allow_multithreading=False)  # Anastasia
    doc3 = Doc(allow_multithreading=False)  # Milica

    # counter maps
    counter_map1 = Map()
    counter_map2 = Map()
    counter_map3 = Map()

    doc1["my_counter"] = counter_map1
    doc2["my_counter"] = counter_map2
    doc3["my_counter"] = counter_map3

    print("\nInitial counter states:")
    print(f"Replica 1 total: {get_total_count(counter_map1)}")
    print(f"Replica 2 total: {get_total_count(counter_map2)}")
    print(f"Replica 3 total: {get_total_count(counter_map3)}")
    print("-" * 30)

    print("\nReplicas increment their local counters independently")

    counter_map1["replica1"] = counter_map1.get("replica1", 0) + 5
    print(f"Replica 1 incremented (+5): {dict(counter_map1)}")

    counter_map2["replica2"] = counter_map2.get("replica2", 0) + 3
    print(f"Replica 2 incremented (+3): {dict(counter_map2)}")

    counter_map3["replica3"] = counter_map3.get("replica3", 0) + 7
    print(f"Replica 3 incremented (+7): {dict(counter_map3)}")
    print("-" * 30)

    print("\n--- Synchronizing replicas ---")

    # Replica 1 -> Replica 2
    state2 = doc2.get_state()
    update1 = doc1.get_update(state2)
    async with doc2.new_transaction():
        doc2.apply_update(update1)
    print(f"Replica 1 -> 2 applied: {dict(counter_map2)}")

    counter_map1["replica1"] = counter_map1.get("replica1") + 5
    print(f"Replica 1 incremented (+5): {dict(counter_map1)}")

    # Replica 2 -> Replica 1
    state1 = doc1.get_state()
    update2 = doc2.get_update(state1)
    async with doc1.new_transaction():
        doc1.apply_update(update2)
    print(f"Replica 2 -> 1 applied: {dict(counter_map1)}")

    print("\nReplica 1 keys:", list(counter_map1.keys()))
    print("Replica 2 keys:", list(counter_map2.keys()))
    print("-" * 30)

    print("\n--- Final synchronization with Replica 3 ---")

    # Replica 1 -> Replica 3
    state3 = doc3.get_state()
    update13 = doc1.get_update(state3)
    async with doc3.new_transaction():
        doc3.apply_update(update13)
    print(f"Replica 1 -> 3 applied: {dict(counter_map3)}")

    # Replica 3 -> Replica 1
    state1 = doc1.get_state()
    update31 = doc3.get_update(state1)
    async with doc1.new_transaction():
        doc1.apply_update(update31)
    print(f"Replica 3 -> 1 applied: {dict(counter_map1)}")

    # Replica 1 -> Replica 2 (final merge)
    state2 = doc2.get_state()
    update12 = doc1.get_update(state2)
    async with doc2.new_transaction():
        doc2.apply_update(update12)
    print(f"Replica 1 -> 2 applied again: {dict(counter_map2)}")
    print("-" * 30)

    print("\n--- Final states ---")
    print(f"Replica 1: {dict(counter_map1)}")
    print(f"Replica 2: {dict(counter_map2)}")
    print(f"Replica 3: {dict(counter_map3)}")

    total1 = get_total_count(counter_map1)
    total2 = get_total_count(counter_map2)
    total3 = get_total_count(counter_map3)

    print(f"\nFinal totals:")
    print(f"Replica 1: {total1}")
    print(f"Replica 2: {total2}")
    print(f"Replica 3: {total3}")

    if total1 == total2 == total3:
        print("\n All replicas successfully converged!")
    else:
        print("\n Replicas did not converge!")


asyncio.run(run_g_counter_example())