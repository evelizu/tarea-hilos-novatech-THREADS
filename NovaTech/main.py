"""
NovaTech - Simulador concurrente de pedidos
Fase 3: Hilo monitor y hilo principal

Este modulo define:
  - La funcion monitor(): un hilo que imprime el estado del sistema cada
    1-2 segundos (pendientes, aprobados, rechazados, trabajadores activos)
    y se detiene mediante una señal (threading.Event), no por conteo de
    iteraciones ni por adivinar cuanto tardaran los workers.
  - main(): el hilo principal, que:
      1. Carga inventario y pedidos (Fase 1).
      2. Crea la cola compartida y el SharedState (Fase 1 + Fase 2).
      3. Crea y arranca 3 hilos trabajadores + 1 hilo monitor.
      4. Espera (join) a que los 3 trabajadores terminen.
      5. Señala al monitor que debe detenerse (Event.set()) y hace join
         sobre el monitor tambien, para garantizar "cierre limpio, sin
         hilos abandonados" (RF-09).
      6. Imprime el resumen final (RF-10).

Decision de diseno: como se notifica al monitor que debe terminar
------------------------------------------------------------------
Se usa un threading.Event (`evento_fin`). El monitor hace un bucle que,
en cada iteracion, espera hasta 1.5s con `evento_fin.wait(timeout=1.5)`:
  - Si el evento se activa antes de que se cumpla el timeout, `wait()`
    retorna True inmediatamente y el monitor sale del bucle sin esperar
    el segundo completo (apagado rapido, no hay que esperar el proximo
    tick).
  - Si el timeout se cumple sin que el evento se active, `wait()` retorna
    False y el monitor simplemente imprime el estado actual y continua.

Esto es preferible a que el monitor revise "cola.empty()" el mismo,
porque la condicion real de finalizacion del sistema es "todos los
trabajadores terminaron", que es una decision que le corresponde al hilo
principal (quien sabe cuando hizo join a los 3 workers), no al monitor
adivinando por su cuenta.
"""

import threading
import time
from queue import Queue

from models import crear_inventario, generar_pedidos, crear_cola_pedidos
from workers import SharedState, trabajador


NUM_TRABAJADORES = 3
INTERVALO_MONITOR_SEGUNDOS = 1.5


def monitor(cola_pedidos: Queue, estado: SharedState, evento_fin: threading.Event):
    """
    Hilo monitor (RF-07): imprime el estado del sistema cada 1.5s
    (dentro del rango 1-2s pedido) hasta recibir la señal de finalizacion.

    Lee: tamano de la cola (pendientes), contadores (aprobados,
    rechazados) y trabajadores_activos. Todas estas lecturas son
    aproximadas/"eventually consistent" a proposito -- el monitor es solo
    observador, no participa en la seccion critica ni bloquea a los
    trabajadores. Por eso no se usa lock_inventario aqui: leer contadores
    con lock_contadores es suficiente y barato.
    """
    estado.registrar_evento("[MONITOR] Hilo monitor iniciado.")

    while not evento_fin.is_set():
        # Espera interrumpible: si la señal llega durante la espera,
        # salimos de inmediato en vez de esperar el intervalo completo.
        señal_recibida = evento_fin.wait(timeout=INTERVALO_MONITOR_SEGUNDOS)
        if señal_recibida:
            break

        with estado.lock_contadores:
            pendientes = cola_pedidos.qsize()
            aprobados = estado.contadores["aprobados"]
            rechazados = estado.contadores["rechazados"]
            activos = estado.trabajadores_activos

        estado.registrar_evento(
            f"[MONITOR] Pendientes: {pendientes} | Aprobados: {aprobados} | "
            f"Rechazados: {rechazados} | Activos: {activos}"
        )

    estado.registrar_evento("[MONITOR] Señal de finalizacion recibida. Hilo monitor detenido.")


def imprimir_resumen_final(estado: SharedState, tiempo_total: float,
                            hilos_secundarios: list, total_pedidos: int):
    """
    RF-10: Muestra inventario restante, aprobados, rechazados, fallidos
    y tiempo total. Tambien reporta cuantos de los hilos secundarios
    (3 workers + 1 monitor = 4) terminaron correctamente, como evidencia
    de "cierre limpio, sin hilos abandonados".
    """
    hilos_finalizados = sum(1 for h in hilos_secundarios if not h.is_alive())
    total_hilos = len(hilos_secundarios)

    print("\n--------------------------------------------------------------------")
    print(
        f"RESUMEN FINAL | Procesados: {estado.contadores['procesados']} | "
        f"Aprobados: {estado.contadores['aprobados']} | "
        f"Rechazados: {estado.contadores['rechazados']} | "
        f"Error: {estado.contadores['error']}"
    )
    print(
        f"Tiempo total: {tiempo_total:.2f} s | "
        f"Hilos finalizados correctamente: {hilos_finalizados}/{total_hilos}"
    )

    # Verificacion de la invariante de integridad pedida en el enunciado:
    # existencia_final = existencia_inicial - unidades_realmente_aprobadas
    print("\nInventario final:")
    for codigo, datos in estado.inventario.items():
        print(f"  {codigo}: {datos['nombre']:<22} existencia={datos['existencia']}")

    # Chequeo de consistencia (>= 0 en todo el inventario, y pedidos
    # procesados == total cargado).
    inventario_valido = all(d["existencia"] >= 0 for d in estado.inventario.values())
    conteo_correcto = estado.contadores["procesados"] == total_pedidos

    print(f"\nInventario sin cantidades negativas: {'OK' if inventario_valido else 'FALLO'}")
    print(f"Pedidos procesados == pedidos cargados ({total_pedidos}): "
          f"{'OK' if conteo_correcto else 'FALLO'}")


def main():
    """
    Hilo principal (RF-01, RF-02, RF-09):
      - Carga datos.
      - Crea 3 workers + 1 monitor (4 hilos secundarios en total, cumple
        el minimo del enunciado).
      - Inicia todos, espera (join) a los workers, señala al monitor,
        hace join al monitor, imprime resumen.
    """
    inventario = crear_inventario()
    pedidos = generar_pedidos()
    cola_pedidos = crear_cola_pedidos(pedidos)
    estado = SharedState(inventario)
    evento_fin = threading.Event()

    print(f"=== NovaTech: procesando {len(pedidos)} pedidos con "
          f"{NUM_TRABAJADORES} trabajadores + 1 monitor ===\n")

    # --- Creacion de hilos ---
    hilos_trabajadores = [
        threading.Thread(
            target=trabajador,
            args=(f"WORKER-{i}", cola_pedidos, estado),
            name=f"WORKER-{i}",
        )
        for i in range(1, NUM_TRABAJADORES + 1)
    ]
    hilo_monitor = threading.Thread(
        target=monitor,
        args=(cola_pedidos, estado, evento_fin),
        name="MONITOR",
    )

    inicio = time.time()

    # --- Inicio: monitor primero para que capture el estado desde el
    #     arranque, luego los trabajadores ---
    hilo_monitor.start()
    for h in hilos_trabajadores:
        h.start()

    # --- Espera a que los trabajadores terminen (RF-09) ---
    for h in hilos_trabajadores:
        h.join()

    # --- Señal de finalizacion al monitor y espera a que se detenga ---
    evento_fin.set()
    hilo_monitor.join()

    fin = time.time()

    # --- Resumen final (RF-10) ---
    todos_los_hilos = hilos_trabajadores + [hilo_monitor]
    imprimir_resumen_final(estado, fin - inicio, todos_los_hilos, len(pedidos))


if __name__ == "__main__":
    main()
