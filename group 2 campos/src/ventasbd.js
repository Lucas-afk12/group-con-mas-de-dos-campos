
/* Esta Base de datos refleja los articulos vendidos el mes 03 de 2020 por parte de una empresa la cual vende articulos
de boxeo y demas deportes de contacto*/

db.sales.insertMany([
    { _id : 1, 
        item : "guante boxeo puma",
        price : NumberDecimal(40.50),
        quantity : NumberInt(50),
        sell_date : ISODate("2020-03-01"),
        cost : NumberDecimal(14),
        client: "Italian Box fighter club"},

    { _id : 2, 
        item : "comba goma reforzada", 
        price : NumberDecimal(12.85), 
        quantity : NumberInt(22), 
        sell_date : ISODate("2020-03-15"),
        cost : NumberDecimal(4.50),
        client: "Club de boxeo utrera"},

    { _id : 3, 
        item : "saco pesado budha", 
        price : NumberDecimal(240),  
        quantity : NumberInt(4), 
        sell_date : ISODate("2020-03-01"),
        cost : NumberDecimal(100),
        client: "Italian Box fighter club"},

    { _id : 4, 
        item : "pack de 6 vendas 5 metros",
        price : NumberDecimal(20),  
        quantity : NumberInt(5) , 
        sell_date : ISODate("2020-03-20"),
        cost : NumberDecimal(6.50),
        client: "obushi fighting club"},

    { _id : 5, 
        item : "bucal t-shark", 
        price : NumberDecimal(8.50), 
        quantity : NumberInt(30) , 
        sell_date : ISODate("2020-03-28"),
        cost : NumberDecimal(4),
        client: "Italian Box fighter club"},

    { _id : 6, 
        item : "guantes venum 2.0 14oz", 
        price : NumberDecimal(56.70),
        quantity : NumberInt(7) , 
        sell_date : ISODate("2020-03-15") ,
        cost : NumberDecimal(22),
        client: "Club de boxeo utrera"},

    { _id : 7, 
        item : "espinillera reforzada anti golpe", 
        price : NumberDecimal(16),
        quantity : NumberInt(20) , 
        sell_date : ISODate("2020-03-20"),
        cost : NumberDecimal(10),
        client: "obushi fighting club"},

    { _id : 8, 
        item : "casco sparring venum", 
        price : NumberDecimal(30), 
        quantity : NumberInt(4) , 
        sell_date : ISODate("2020-03-13"),
        cost : NumberDecimal(10),
        client: "Club boxeo utrera"},
  ])