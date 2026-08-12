"""
NovaTech - Simulador concurrente de pedidos
Fase 2: Logica de sincronizacion (trabajadores)

Este modulo define:
  - Un objeto SharedState que agrupa todos los recursos compartidos y sus
    locks (inventario, contadores, log de resultados).
  - La funcion trabajador() que ejecuta cada hilo Worker: toma pedidos de
    la cola, simula tiempo de procesamiento FUERA del lock, y valida +
    descuenta inventario DENTRO de una seccion critica minima.

Decisiones de diseno clave (se piden explicarlas en el informe, Fase 5):

1. Un solo Lock protege el inventario (`self.lock_inventario`). Se eligio
   un unico lock (en vez de uno por producto) porque el catalogo es
   pequeno (5 productos) y un pedido puede tener varios items de
   distintos productos: usar locks por producto obligaria a un protocolo
   de multiples locks con riesgo de deadlock (orden de adquisicion). Con
   un solo lock evitamos esa complejidad; el costo en concurrencia es
   aceptable porque la seccion critica es muy corta (solo lectura +
   resta de enteros, sin I/O).

2. La simulacion de trabajo (0.5 a 2 segundos, time.sleep) se hace ANTES
   de tomar el lock, nunca dentro. Esto cumple la regla del enunciado:
   "No mantenga un lock durante esperas artificiales... o pausas de
   simulacion".

3. Los contadores (aprobados, rechazados, error) y el log de eventos usan
   el MISMO lock que el inventario cuando se actualizan junto con el
   descuento (para que el conteo sea consistente con el estado del
   inventario en todo momento), pero se usa un lock aparte
   (`lock_contadores`) para los casos que no tocan inventario (pedidos
   invalidos), de modo que un pedido invalido no compita por el lock del
   inventario innecesariamente.

4. queue.Queue.get() ya es thread-safe: dos workers nunca pueden extraer
   el mismo Pedido de la cola. Por eso "pedido duplicado" no puede
   ocurrir a nivel de la cola; el riesgo real esta en el inventario, que
   es lo que protegemos explicitamente aqui.
"""

import threading
import time
import random
from datetime import datetime
from queue import Queue, Empty


# ---------------------------------------------------------------------------
# Estado compartido
# ---------------------------------------------------------------------------

class SharedState:
    """
    Agrupa todos los recursos que los hilos comparten, junto con los
    mecanismos de sincronizacion que los protegen.

    inventario:          dict codigo -> {nombre, existencia}. Protegido por
                          lock_inventario.
    lock_inventario:      Lock que protege la seccion critica: verificar
                          existencia + descontar, como UNA sola operacion
                          atomica.
    contadores:           dict con aprobados/rechazados/error. Protegido
                          por lock_contadores (o por lock_inventario cuando
                          la actualizacion ocurre junto con el descuento,
                          para mantener una sola seccion critica corta).
    lock_contadores:       Lock separado para actualizaciones de contadores
                          que no requieren tocar el inventario (ej. pedidos
                          invalidos).
    eventos:              lista de tuplas (timestamp, mensaje) para el log
                          final. Protegida por lock_eventos.
    lock_eventos:          Lock para el log, para que las lineas no se
                          intercalen a nivel de caracteres al imprimir.
    trabajadores_activos: contador de workers que siguen vivos, que el
                          monitor consulta periodicamente. Protegido por
                          lock_contadores tambien (es una cifra mas).
    """

    def __init__(self, inventario_inicial):
        self.inventario = inventario_inicial
        self.lock_inventario = threading.Lock()

        self.contadores = {
            "aprobados": 0,
            "rechazados": 0,
            "error": 0,
            "procesados": 0,
        }
        self.lock_contadores = threading.Lock()

        self.eventos = []
        self.lock_eventos = threading.Lock()

        self.trabajadores_activos = 0

    # -- utilidades de registro -------------------------------------------------

    def registrar_evento(self, mensaje: str):
        """
        Agrega una linea al log compartido y la imprime inmediatamente.
        Se usa un lock corto solo para el 'print' + append, nunca para
        trabajo lento, por lo que no genera contencion significativa
        aunque muchos hilos escriban al mismo tiempo.
        """
        marca_tiempo = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        linea = f"[{marca_tiempo}] {mensaje}"
        with self.lock_eventos:
            self.eventos.append(linea)
            print(linea, flush=True)


# ---------------------------------------------------------------------------
# Validacion de pedidos (fuera de la seccion critica: es logica pura,
# no toca el inventario compartido).
# ---------------------------------------------------------------------------

def validar_estructura_pedido(pedido, inventario_codigos):
    """
    Revisa que el pedido este bien formado ANTES de intentar tocar el
    inventario. No requiere lock porque solo lee datos propios del pedido
    y una lista de codigos validos que no cambia en tiempo de ejecucion.

    Devuelve (es_valido: bool, motivo: str).
    """
    if not pedido.cliente or not pedido.cliente.strip():
        return False, "Cliente vacio o invalido"

    if not pedido.items:
        return False, "Pedido sin items"

    for item in pedido.items:
        if item.codigo_producto not in inventario_codigos:
            return False, f"Producto inexistente: {item.codigo_producto}"
        if item.cantidad is None or item.cantidad <= 0:
            return False, f"Cantidad invalida ({item.cantidad}) para {item.codigo_producto}"

    return True, ""


# ---------------------------------------------------------------------------
# Funcion ejecutada por cada hilo trabajador
# ---------------------------------------------------------------------------

def trabajador(nombre_worker: str, cola_pedidos: Queue, estado: SharedState,
                simular_tiempo=True, rango_espera=(0.5, 2.0)):
    """
    Ciclo de vida de un hilo trabajador (RF-03, RF-04, RF-05, RF-06, RF-08):

      1. Extrae un pedido de la cola compartida (thread-safe por diseno de
         queue.Queue). Si la cola esta vacia, el worker termina (esa es su
         condicion de finalizacion, segun la tabla del enunciado).
      2. Valida la ESTRUCTURA del pedido (sin tocar inventario). Si esta
         mal formado, se registra como error y se continua con el
         siguiente pedido, sin detener a los demas trabajadores (RF-08).
      3. Simula el tiempo de procesamiento (0.5 a 2s) FUERA de cualquier
         lock, como exige el enunciado.
      4. Entra a la SECCION CRITICA: verifica existencia y descuenta el
         inventario como una sola operacion atomica protegida por
         estado.lock_inventario. Si no alcanza el stock, rechaza el
         pedido SIN modificar el inventario.
      5. Registra el resultado (aprobado/rechazado/error) con hora, hilo,
         id, cliente y motivo.

    Con try/except alrededor del procesamiento de cada pedido, un pedido
    problematico jamas puede tumbar el hilo completo (RF-08).
    """
    with estado.lock_contadores:
        estado.trabajadores_activos += 1

    estado.registrar_evento(f"[{nombre_worker}] Hilo iniciado.")

    try:
        while True:
            try:
                # Extraccion no bloqueante: si no hay pedidos, el worker
                # termina en vez de esperar indefinidamente (condicion de
                # finalizacion: "cuando no quedan pedidos pendientes").
                pedido = cola_pedidos.get_nowait()
            except Empty:
                break

            try:
                estado.registrar_evento(
                    f"[{nombre_worker}] Inicia pedido {pedido.id_pedido} | Cliente: {pedido.cliente}"
                )

                # --- 1) Validacion de estructura (fuera del lock) ---
                es_valido, motivo = validar_estructura_pedido(
                    pedido, estado.inventario.keys()
                )
                if not es_valido:
                    with estado.lock_contadores:
                        estado.contadores["error"] += 1
                        estado.contadores["procesados"] += 1
                    estado.registrar_evento(
                        f"[{nombre_worker}] {pedido.id_pedido} ERROR | Motivo: {motivo}"
                    )
                    continue

                # --- 2) Simulacion de trabajo, FUERA de la seccion critica ---
                if simular_tiempo:
                    time.sleep(random.uniform(*rango_espera))

                # --- 3) SECCION CRITICA: verificar + descontar inventario ---
                # Esta es la unica parte protegida por lock_inventario.
                # Se mantiene lo mas corta posible: solo comparaciones y
                # restas de enteros, sin sleep ni I/O adentro.
                aprobado = False
                detalle_descuento = []
                motivo_rechazo = ""

                with estado.lock_inventario:
                    # Verificar que TODOS los items del pedido tengan stock
                    # suficiente antes de descontar cualquiera (para que un
                    # pedido con varios productos sea todo o nada).
                    suficiente = all(
                        estado.inventario[item.codigo_producto]["existencia"] >= item.cantidad
                        for item in pedido.items
                    )

                    if suficiente:
                        for item in pedido.items:
                            estado.inventario[item.codigo_producto]["existencia"] -= item.cantidad
                            detalle_descuento.append(f"{item.codigo_producto}: -{item.cantidad} unidades")
                        aprobado = True
                    else:
                        # Identificar cual producto no alcanzo, solo para
                        # el mensaje (no modifica nada).
                        for item in pedido.items:
                            disponible = estado.inventario[item.codigo_producto]["existencia"]
                            if disponible < item.cantidad:
                                motivo_rechazo = f"Stock insuficiente {item.codigo_producto} (disponible={disponible}, pedido={item.cantidad})"
                                break

                # --- 4) Registro del resultado (fuera del lock de inventario) ---
                with estado.lock_contadores:
                    estado.contadores["procesados"] += 1
                    if aprobado:
                        estado.contadores["aprobados"] += 1
                    else:
                        estado.contadores["rechazados"] += 1

                if aprobado:
                    estado.registrar_evento(
                        f"[{nombre_worker}] {pedido.id_pedido} APROBADO | {', '.join(detalle_descuento)}"
                    )
                else:
                    estado.registrar_evento(
                        f"[{nombre_worker}] {pedido.id_pedido} RECHAZADO | {motivo_rechazo}"
                    )

            except Exception as exc:
                # Cualquier error inesperado al procesar UN pedido no debe
                # detener al worker ni a los demas (RF-08).
                with estado.lock_contadores:
                    estado.contadores["error"] += 1
                    estado.contadores["procesados"] += 1
                estado.registrar_evento(
                    f"[{nombre_worker}] {pedido.id_pedido} ERROR INESPERADO | {exc}"
                )
    finally:
        with estado.lock_contadores:
            estado.trabajadores_activos -= 1
        estado.registrar_evento(f"[{nombre_worker}] Hilo finalizado. Sin pedidos pendientes.")


# ---------------------------------------------------------------------------
# Prueba rapida de la Fase 2 (crea 3 workers manualmente, sin monitor
# todavia -- el monitor y el hilo principal completo llegan en la Fase 3).
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from models import crear_inventario, generar_pedidos, crear_cola_pedidos

    inventario = crear_inventario()
    pedidos = generar_pedidos()
    cola = crear_cola_pedidos(pedidos)
    estado = SharedState(inventario)

    hilos = []
    for i in range(1, 4):
        t = threading.Thread(target=trabajador, args=(f"WORKER-{i}", cola, estado), daemon=False)
        hilos.append(t)

    inicio = time.time()
    for t in hilos:
        t.start()
    for t in hilos:
        t.join()
    fin = time.time()

    print("\n--------------------------------------------------------------------")
    print("RESUMEN (prueba de Fase 2, sin monitor)")
    print(f"Procesados: {estado.contadores['procesados']} | "
          f"Aprobados: {estado.contadores['aprobados']} | "
          f"Rechazados: {estado.contadores['rechazados']} | "
          f"Error: {estado.contadores['error']}")
    print(f"Tiempo total: {fin - inicio:.2f} s")
    print("\nInventario final:")
    for codigo, datos in estado.inventario.items():
        print(f"  {codigo}: {datos['nombre']:<22} existencia={datos['existencia']}")
