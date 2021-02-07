/*group con 3 campos el cual recoje la fecha el producto y la cantidad de productos*/
db.sales.aggregate([
/*El primer match filtra por productos los cuales se hayan vendido mas de 20 unidades */
    {
        $match:{
            "quantity":{$gte:20}
        }
    },
    {
        $group:{
            _id:{
            fecha:"$sell_date",
            producto:"$item",
            cantidad:"$quantity",
        },
        dinero:{$sum:{$multiply: [ "$price", "$quantity" ] }} ,  
    }
    },
    /*En el proyect calculamos el iva, el total de iva y lo redondeamos ademas aplicamos un descuento */
    {
        $project:{
            
            _id:0,
            año:"$_id.fecha",
            cantidad_de_ventas:"$_id.cantidad",
            Producto:"$_id.producto",
            total_ventas:"$dinero",
            Iva:{$multiply:["$dinero",0.21]},
            Total_iva:{$multiply:["$dinero",1.21]},
            total_redondeado:{$round:[{$multiply:["$dinero",1.21]}]},
            /*Si el total de ventas es mayor a 300 euros se le hara un descuento del 20% mientras que si es menos se le hara un descuento del 10%*/
            descuento:  {
                
                $cond: {
                    if: { $gte: ["$DINERO", 300] },
                    then: { $multiply: ["$dinero", NumberDecimal("0.2")] },
                    else: { $multiply: ["$dinero", NumberDecimal("0.1")] }
                }}
        }
    },
    {
        $sort:{fecha:1,producto:1}
    },
    /*este match selecciona a traves del descuento hecho anteriormente en el project seleccionando los productos en los que se haya descontado mas de 30 euros */
    {
        $match:{
            $expr: { $gt:[ "$descuento",  NumberDecimal("30") ] } 
        }
    }

]).pretty();