"""
NovaTech - Simulador concurrente de pedidos
Fase 1: Estructuras de datos e inventario

Este modulo define:
  - El inventario inicial (compartido entre hilos).
  - La estructura de un Pedido.
  - La generacion de al menos 20 pedidos de prueba.
  - La cola compartida (thread-safe) que usaran los trabajadores.

Nota de diseno: separamos "datos" (este archivo) de "sincronizacion"
(Fase 2), para que el lock que protege el inventario se defina y se use
junto con la logica de los trabajadores, no aqui.
"""

from dataclasses import dataclass, field
from queue import Queue
import random


# ---------------------------------------------------------------------------
# Inventario inicial
# ---------------------------------------------------------------------------
# Se representa como un diccionario simple: codigo -> {nombre, existencia}.
# Este diccionario sera el "recurso compartido" que los trabajadores leeran
# y modificaran en la Fase 2, protegido por un Lock.

INVENTARIO_INICIAL = {
    "P001": {"nombre": "Teclado mecanico",     "existencia": 12},
    "P002": {"nombre": "Mouse inalambrico",    "existencia": 18},
    "P003": {"nombre": "Audifonos USB",        "existencia": 10},
    "P004": {"nombre": "Camara web",           "existencia": 8},
    "P005": {"nombre": "Monitor de 24 pulgadas","existencia": 6},
}


def crear_inventario():
    """
    Devuelve una copia nueva del inventario inicial.

    Usamos una funcion (en vez de reusar el diccionario global directamente)
    para que cada ejecucion del programa (o cada caso de prueba) empiece
    desde un estado limpio, sin arrastrar cambios de una corrida anterior.
    """
    return {
        codigo: {"nombre": datos["nombre"], "existencia": datos["existencia"]}
        for codigo, datos in INVENTARIO_INICIAL.items()
    }


# ---------------------------------------------------------------------------
# Estructura de un pedido
# ---------------------------------------------------------------------------

@dataclass
class ItemPedido:
    """Un producto solicitado dentro de un pedido, con su cantidad."""
    codigo_producto: str
    cantidad: int


@dataclass
class Pedido:
    """
    Representa un pedido del cliente.

    id_pedido:  identificador unico (ej. 'ORD-007').
    cliente:    nombre del cliente.
    items:      lista de ItemPedido (un pedido puede tener 1+ productos).
    valido:     bandera que marcamos en False si el pedido viene mal
                formado (sin codigo, cantidad <= 0, etc.). Esto nos permite
                generar pedidos invalidos a proposito para el caso de
                prueba CP-04, sin romper la carga de datos.
    """
    id_pedido: str
    cliente: str
    items: list = field(default_factory=list)
    valido: bool = True
    motivo_invalido: str = ""


# ---------------------------------------------------------------------------
# Generacion de pedidos de prueba (>= 20, con casos normales, de
# contencion, de stock insuficiente y malformados)
# ---------------------------------------------------------------------------

def generar_pedidos(cantidad: int = 24, semilla: int = 42):
    """
    Genera una lista de pedidos de prueba.

    Se incluyen intencionalmente:
      - Pedidos normales (stock suficiente).
      - Varios pedidos que compiten por el mismo producto con poca
        existencia (para forzar contencion real sobre el lock, ej. P005
        que solo tiene 6 unidades).
      - Al menos un pedido que pide mas unidades de las disponibles.
      - Al menos un pedido invalido (codigo inexistente, cantidad 0 o
        estructura incompleta) para el caso de prueba CP-04.

    Se usa una semilla fija para que la ejecucion sea reproducible,
    tal como exige el README/rubrica.
    """
    random.seed(semilla)
    nombres_clientes = [
        "Ana Lopez", "Mario Perez", "Lucia Gomez", "Carlos Ruiz",
        "Elena Torres", "Diego Ramirez", "Sofia Castillo", "Jorge Diaz",
        "Valeria Mendez", "Pablo Rios", "Camila Ortiz", "Andres Vargas",
    ]
    codigos_validos = list(INVENTARIO_INICIAL.keys())

    pedidos = []

    # 1) Pedidos normales: cantidades pequenas, dentro de la existencia.
    for i in range(1, 13):
        codigo = random.choice(codigos_validos)
        cantidad_pedida = random.randint(1, 3)
        pedidos.append(Pedido(
            id_pedido=f"ORD-{i:03d}",
            cliente=random.choice(nombres_clientes),
            items=[ItemPedido(codigo, cantidad_pedida)],
        ))

    # 2) Pedidos de contencion: varios compiten por P005 (solo 6 unidades)
    #    para forzar que el mecanismo de sincronizacion se ejercite de
    #    verdad (CP-02).
    for i in range(13, 18):
        pedidos.append(Pedido(
            id_pedido=f"ORD-{i:03d}",
            cliente=random.choice(nombres_clientes),
            items=[ItemPedido("P005", 2)],  # 5 pedidos x 2 = 10 > 6 disponibles
        ))

    # 3) Pedido con stock claramente insuficiente (CP-03).
    pedidos.append(Pedido(
        id_pedido="ORD-018",
        cliente="Mariana Solis",
        items=[ItemPedido("P004", 999)],
    ))

    # 4) Pedidos invalidos / mal formados (CP-04): codigo inexistente,
    #    cantidad cero y cantidad negativa.
    pedidos.append(Pedido(
        id_pedido="ORD-019",
        cliente="Cliente Fantasma",
        items=[ItemPedido("P999", 1)],  # producto que no existe
    ))
    pedidos.append(Pedido(
        id_pedido="ORD-020",
        cliente="Roberto Nunez",
        items=[ItemPedido("P002", 0)],  # cantidad invalida
    ))
    pedidos.append(Pedido(
        id_pedido="ORD-021",
        cliente="",  # cliente vacio -> tambien invalido
        items=[ItemPedido("P001", -2)],
    ))

    # 5) Rellenar hasta 'cantidad' con pedidos normales adicionales, por si
    #    se pide un total mayor a 21.
    siguiente = 22
    while len(pedidos) < cantidad:
        codigo = random.choice(codigos_validos)
        pedidos.append(Pedido(
            id_pedido=f"ORD-{siguiente:03d}",
            cliente=random.choice(nombres_clientes),
            items=[ItemPedido(codigo, random.randint(1, 2))],
        ))
        siguiente += 1

    return pedidos


def crear_cola_pedidos(pedidos):
    """
    Coloca la lista de pedidos dentro de una queue.Queue, que es thread-safe
    por diseño (usa un lock internamente). Esta sera la 'Cola compartida de
    pedidos' que los 3+ trabajadores consumiran concurrentemente en la
    Fase 2, cada uno haciendo cola.get() sin riesgo de tomar el mismo
    pedido dos veces.
    """
    cola = Queue()
    for pedido in pedidos:
        cola.put(pedido)
    return cola


# ---------------------------------------------------------------------------
# Prueba rapida de la Fase 1 (se puede ejecutar este archivo directamente
# para verificar que los datos se generan correctamente antes de pasar a
# la logica de hilos).
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    inventario = crear_inventario()
    pedidos = generar_pedidos()
    cola = crear_cola_pedidos(pedidos)

    print("=== INVENTARIO INICIAL ===")
    for codigo, datos in inventario.items():
        print(f"  {codigo}: {datos['nombre']:<22} existencia={datos['existencia']}")

    print(f"\n=== PEDIDOS GENERADOS: {len(pedidos)} ===")
    for p in pedidos:
        items_str = ", ".join(f"{it.codigo_producto}x{it.cantidad}" for it in p.items)
        print(f"  {p.id_pedido} | cliente='{p.cliente}' | items=[{items_str}]")

    print(f"\nTamano de la cola compartida: {cola.qsize()} pedidos listos para procesar.")
