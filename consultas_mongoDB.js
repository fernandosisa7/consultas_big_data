//Consultas MongoDB
// 1. Inserción de un dato
const insercion = db.compras.insertOne({
    fecha_compra: new Date('2024-11-19'),
    nombre_cliente: 'Ana Rodríguez',
    edad_cliente: 28,
    producto: 'Audífonos',
    categoria: 'Tecnología',
    marca: 'Sony',
    modelo: 'WH-1000XM5',
    precio: 350.00,
    cantidad: 1,
    metodo_pago: 'Transferencia bancaria',
    direccion_envio: 'Calle 26 # 50-30, Medellín, Colombia',
    estado_envio: 'Pendiente'
});
print("Inserción realizada:");
printjson(insercion);

// 2. Selección de documentos
const seleccion = db.compras.find({ estado_envio: 'Enviado' }).pretty();
print("Documentos seleccionados:");
seleccion.forEach(printjson); // Iterar y mostrar documentos

// 3. Actualización de un documento
const actualizacion = db.compras.updateOne(
    { nombre_cliente: 'Carlos Martínez' }, // Filtro
    { $set: { estado_envio: 'Entregado' } } // Actualización
);
print("Actualización realizada:");
printjson(actualizacion);

// 4. Eliminación de un documento
const eliminacion = db.compras.deleteOne({ nombre_cliente: 'Ana Rodríguez' });
print("Eliminación realizada:");
printjson(eliminacion);

//Consulta con filtro
const filtro = db.compras.find({
    edad_cliente: { $gte: 40 }, // Edad mayor o igual a 40
    categoria: 'Tecnología'     // Categoría específica
}).pretty();
print("Consulta con filtros (clientes >= 30 años y categoría 'Tecnología'):");
filtro.forEach(printjson);

// Consulta con Operadores
const operadores = db.compras.find({
    precio: { $gt: 800 },          // Precio mayor a 800
    estado_envio: { $ne: 'Entregado' } // Estado diferente de "Entregado"
}).pretty();
print("Consulta con operadores (precio > 800 y estado != 'Entregado'):");
operadores.forEach(printjson);

//Metodo de pago mas utilizado
// Agrupa las compras por método de pago y cuenta cuántas veces se usó cada uno. Esto ayuda a identificar los métodos de pago preferidos por los clientes.
db.compras.aggregate([
    { $group: { _id: "$metodo_pago", uso_metodo_pago: { $sum: 1 } } },
    { $sort: { uso_metodo_pago: -1 } }
]);

// numero de registros con estado Enviado o Entregado
db.compras.countDocuments({
    estado_envio: { $in: ["Enviado", "Entregado"] }
});