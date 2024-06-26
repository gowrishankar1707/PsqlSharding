const app=require("express")();
/** databases */
const {Client}=require("pg");
const ConsistentHash=require("hashring");
const crypto=require("crypto");
const { url } = require("inspector");
//created a hash ring
const hr=new ConsistentHash();
// adding the 3 port instances
hr.add("5432")
hr.add("5433")
hr.add("5434")

// adding the 3 clients information including the host,port,user,password,database
const clients={
    "5432":new Client({
        "host":"192.168.1.2",
        "port":"5432",
        "user":"postgres",
        "password":"postgres",
        "database":"postgres"
    }),
    "5433":new Client({
        "host":"192.168.1.2",
        "port":"5433",
        "user":"postgres",
        "password":"postgres",
        "database":"postgres"
    }),
    "5434":new Client({
        "host":"192.168.1.2",
        "port":"5434",
        "user":"postgres",
        "password":"postgres",
        "database":"postgres"
    })
}
connect();
async function connect(){
    clients["5432"].connect();
    clients["5433"].connect();
    clients["5434"].connect();
}
app.get("/:urlId",async(req,res)=>{
    //http://localhost:8082/1234
    const urlId=req.params.urlId;
    //get the hash value
    const server=hr.get(urlId);
    //query the respective server on Clients object
    const psqlGetQuery="Select Id,Url from URL_TABLE where url_id=$1";
    const getValues=[urlId];

    const results=await clients[server].query(psqlGetQuery,getValues);
    if(results.rowCount>0)
        {
            results.rows.forEach((row, index) => {
                console.log(`Row ${index + 1}:`, row);
              });
        }
        else{
            console.log("There is no row:$1",[urlId]);
        }
    res.send("a");
  
})

app.post("/",async(req,res)=>{
    
    const url = req.query.url;
    // we are using crpto to do hashing
    //consistently hash this to get a port!
    const hash=crypto.createHash("sha256").update(url).digest("base64");
    const urlId=hash.substring(0,5);
    //Getting servers with respective to urlId
    const server=hr.get(urlId);
    // write the URL,URL_ID to the sharded tables
    const psqlQuery='INSERT INTO URL_TABLE(URL,URL_ID) VALUES($1,$2)';
    const value=[url,urlId];
    await clients[server].query(psqlQuery,value);
   // await clients[server].query("INSERT INTO URL_TABLE(URL,URL_ID) VALUES($1,$2)",[url,urlId]);
    res.send({
        "urlId":urlId,
        "URL":url,
        "server":server

    })
    
})

app.listen("8082",()=> console.log("listening chuti"))