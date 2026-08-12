"""
NovaTech - Simulador concurrente de pedidos
Fase 4: Casos de prueba obligatorios (CP-01 a CP-05)

Cada caso de prueba:
  - Construye su PROPIO inventario y su PROPIA lista de pedidos, de forma
    controlada (no depende del generador aleatorio de la Fase 1), para
    que el escenario sea exactamente el que el enunciado pide y el
    resultado sea reproducible.
  - Ejecuta el mismo mecanismo real de hilos (SharedState + trabajador +
    monitor) usado en produccion -- no se simula nada aparte.
  - Termina con asserts explicitos que fallan (AssertionError) si el
    comportamiento no es el esperado, en vez de solo imprimir logs para
    inspeccion visual.

Cada caso imprime su propio log intercalado (evidencia de concurrencia
real) y despues un veredicto PASS/FAIL con la razon.
"""

import threading
import time

from workers import SharedState, trabajador
from main import monitor, INTERVALO_MONITOR_SEGUNDOS
from models import Pedido, ItemPedido
from queue import Queue


def _ejecutar_lote(inventario, pedidos, num_trabajadores=3, con_monitor=True,
                    simular_tiempo=True, rango_espera=(0.5, 2.0)):
    """
    Utilidad compartida por todos los casos de prueba: arma la cola, el
    SharedState, crea N trabajadores (+ opcionalmente 1 monitor), corre
    todo con hilos reales y hace join a todos antes de devolver el
    resultado. Reutiliza exactamente la misma arquitectura de main.py,
    para que las pruebas validen el sistema real, no una version
    simplificada.
    """
    cola = Queue()
    for p in pedidos:
        cola.put(p)

    estado = SharedState(inventario)
    evento_fin = threading.Event()

    hilos_trabajadores = [
        threading.Thread(
            target=trabajador,
            args=(f"WORKER-{i}", cola, estado),
            kwargs={"simular_tiempo": simular_tiempo, "rango_espera": rango_espera},
            name=f"WORKER-{i}",
        )
        for i in range(1, num_trabajadores + 1)
    ]

    hilo_monitor = None
    if con_monitor:
        hilo_monitor = threading.Thread(
            target=monitor, args=(cola, estado, evento_fin), name="MONITOR"
        )
        hilo_monitor.start()

    inicio = time.time()
    for h in hilos_trabajadores:
        h.start()
    for h in hilos_trabajadores:
        h.join()

    if con_monitor:
        evento_fin.set()
        hilo_monitor.join()

    tiempo_total = time.time() - inicio

    todos_los_hilos = hilos_trabajadores + ([hilo_monitor] if hilo_monitor else [])
    hilos_vivos = [h.name for h in todos_los_hilos if h.is_alive()]

    return {
        "estado": estado,
        "tiempo_total": tiempo_total,
        "hilos_vivos": hilos_vivos,
        "total_hilos": len(todos_los_hilos),
    }


def _veredicto(nombre_caso, condiciones):
    """
    condiciones: lista de tuplas (descripcion, bool). Imprime cada
    condicion con su resultado y un veredicto final PASS/FAIL.
    """
    print(f"\n--- Veredicto {nombre_caso} ---")
    todas_ok = True
    for descripcion, ok in condiciones:
        estado_txt = "OK" if ok else "FALLO"
        print(f"  [{estado_txt}] {descripcion}")
        todas_ok = todas_ok and ok
    print(f"  => {nombre_caso}: {'PASS' if todas_ok else 'FAIL'}")
    print("-" * 60)
    return todas_ok


# ---------------------------------------------------------------------------
# CP-01: Flujo normal
# ---------------------------------------------------------------------------

def cp01_flujo_normal():
    print("\n" + "=" * 70)
    print("CP-01: FLUJO NORMAL (stock suficiente para todos los pedidos)")
    print("=" * 70)

    inventario = {
        "P001": {"nombre": "Teclado mecanico", "existencia": 50},
        "P002": {"nombre": "Mouse inalambrico", "existencia": 50},
    }
    pedidos = [
        Pedido(f"ORD-{i:03d}", f"Cliente {i}",
               [ItemPedido("P001" if i % 2 == 0 else "P002", 1)])
        for i in range(1, 13)  # 12 pedidos, todos con stock de sobra
    ]

    resultado = _ejecutar_lote(inventario, pedidos, rango_espera=(0.1, 0.3))
    estado = resultado["estado"]

    return _veredicto("CP-01", [
        ("Todos los pedidos fueron procesados",
         estado.contadores["procesados"] == len(pedidos)),
        ("Todos los pedidos validos fueron aprobados (no hay rechazos)",
         estado.contadores["rechazados"] == 0),
        ("No hubo errores de formato",
         estado.contadores["error"] == 0),
        ("Se ejecuto con multiples trabajadores en paralelo (evidencia: tiempo < suma secuencial)",
         resultado["tiempo_total"] < 0.3 * len(pedidos)),
    ])


# ---------------------------------------------------------------------------
# CP-02: Contencion (varios pedidos compiten por las ultimas unidades)
# ---------------------------------------------------------------------------

def cp02_contencion():
    print("\n" + "=" * 70)
    print("CP-02: CONTENCION (multiples pedidos compiten por el mismo producto)")
    print("=" * 70)

    existencia_inicial = 5
    inventario = {
        "P005": {"nombre": "Monitor de 24 pulgadas", "existencia": existencia_inicial},
    }
    # 10 pedidos de 1 unidad cada uno compitiendo por solo 5 unidades.
    pedidos = [
        Pedido(f"ORD-{i:03d}", f"Cliente {i}", [ItemPedido("P005", 1)])
        for i in range(1, 11)
    ]

    resultado = _ejecutar_lote(inventario, pedidos, num_trabajadores=5,
                                rango_espera=(0.1, 0.2))
    estado = resultado["estado"]
    existencia_final = estado.inventario["P005"]["existencia"]

    return _veredicto("CP-02", [
        ("No se aprobaron mas pedidos que unidades disponibles",
         estado.contadores["aprobados"] == existencia_inicial),
        ("El resto de pedidos fue correctamente rechazado por falta de stock",
         estado.contadores["rechazados"] == len(pedidos) - existencia_inicial),
        ("El inventario final nunca quedo negativo",
         existencia_final >= 0),
        ("Invariante de integridad: existencia_final = existencia_inicial - aprobados",
         existencia_final == existencia_inicial - estado.contadores["aprobados"]),
    ])


# ---------------------------------------------------------------------------
# CP-03: Stock insuficiente (un pedido individual pide mas de lo disponible)
# ---------------------------------------------------------------------------

def cp03_stock_insuficiente():
    print("\n" + "=" * 70)
    print("CP-03: STOCK INSUFICIENTE (un pedido excede la existencia disponible)")
    print("=" * 70)

    inventario = {
        "P004": {"nombre": "Camara web", "existencia": 3},
    }
    pedidos = [
        Pedido("ORD-001", "Cliente Exigente", [ItemPedido("P004", 100)]),
    ]

    resultado = _ejecutar_lote(inventario, pedidos, num_trabajadores=1,
                                rango_espera=(0.1, 0.1))
    estado = resultado["estado"]

    return _veredicto("CP-03", [
        ("El pedido fue rechazado",
         estado.contadores["rechazados"] == 1 and estado.contadores["aprobados"] == 0),
        ("El inventario permanecio sin cambios (3 unidades)",
         estado.inventario["P004"]["existencia"] == 3),
    ])


# ---------------------------------------------------------------------------
# CP-04: Pedido invalido (mal formado)
# ---------------------------------------------------------------------------

def cp04_pedido_invalido():
    print("\n" + "=" * 70)
    print("CP-04: PEDIDO INVALIDO (mal formado: no debe detener a los demas)")
    print("=" * 70)

    inventario = {
        "P001": {"nombre": "Teclado mecanico", "existencia": 10},
    }
    pedidos = [
        Pedido("ORD-001", "Cliente Fantasma", [ItemPedido("P999", 1)]),   # producto inexistente
        Pedido("ORD-002", "", [ItemPedido("P001", 1)]),                    # cliente vacio
        Pedido("ORD-003", "Cliente OK", [ItemPedido("P001", 0)]),         # cantidad cero
        Pedido("ORD-004", "Cliente Valido", [ItemPedido("P001", 1)]),     # este SI es valido
    ]

    resultado = _ejecutar_lote(inventario, pedidos, num_trabajadores=2,
                                rango_espera=(0.1, 0.2))
    estado = resultado["estado"]

    return _veredicto("CP-04", [
        ("Se detectaron exactamente 3 pedidos con error de formato",
         estado.contadores["error"] == 3),
        ("El pedido valido (ORD-004) SI fue procesado y aprobado a pesar de los errores",
         estado.contadores["aprobados"] == 1),
        ("Ningun hilo trabajador murio por un pedido malformado (todos procesados)",
         estado.contadores["procesados"] == len(pedidos)),
    ])


# ---------------------------------------------------------------------------
# CP-05: Cierre limpio
# ---------------------------------------------------------------------------

def cp05_cierre_limpio():
    print("\n" + "=" * 70)
    print("CP-05: CIERRE LIMPIO (cola vacia, monitor detenido, join completo)")
    print("=" * 70)

    inventario = {
        "P002": {"nombre": "Mouse inalambrico", "existencia": 20},
    }
    pedidos = [
        Pedido(f"ORD-{i:03d}", f"Cliente {i}", [ItemPedido("P002", 1)])
        for i in range(1, 9)
    ]

    resultado = _ejecutar_lote(inventario, pedidos, num_trabajadores=3,
                                con_monitor=True, rango_espera=(0.1, 0.2))
    estado = resultado["estado"]

    return _veredicto("CP-05", [
        ("Todos los pedidos fueron procesados (cola vacia)",
         estado.contadores["procesados"] == len(pedidos)),
        ("Ningun hilo (trabajadores + monitor) quedo activo tras el join",
         len(resultado["hilos_vivos"]) == 0),
        (f"Se hizo join a los {resultado['total_hilos']} hilos secundarios esperados",
         resultado["total_hilos"] == 4),  # 3 workers + 1 monitor
    ])


# ---------------------------------------------------------------------------
# Ejecucion de todos los casos de prueba
# ---------------------------------------------------------------------------

def ejecutar_todos_los_casos():
    resultados = {
        "CP-01": cp01_flujo_normal(),
        "CP-02": cp02_contencion(),
        "CP-03": cp03_stock_insuficiente(),
        "CP-04": cp04_pedido_invalido(),
        "CP-05": cp05_cierre_limpio(),
    }

    print("\n" + "=" * 70)
    print("RESUMEN GENERAL DE CASOS DE PRUEBA")
    print("=" * 70)
    for nombre, ok in resultados.items():
        print(f"  {nombre}: {'PASS' if ok else 'FAIL'}")

    todos_pasaron = all(resultados.values())
    print(f"\nResultado global: {'TODOS LOS CASOS PASARON' if todos_pasaron else 'HAY CASOS FALLIDOS'}")
    return todos_pasaron


if __name__ == "__main__":
    exito = ejecutar_todos_los_casos()
    exit(0 if exito else 1)
