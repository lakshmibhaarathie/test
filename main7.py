from typing import Optional
from fastapi import FastAPI,Response,status,HTTPException
from fastapi.params import Body
from pydantic import BaseModel
from random import randrange
import psycopg2
from psycopg2.extras import RealDictCursor
import time

while True:
    try:
        con=psycopg2.connect(host='localhost',user='postgres',database='fastapi',password='sql123',cursor_factory=RealDictCursor)
        cur=con.cursor()
        print("connected to DB..!")
        break
    except Exception as e:
        print('connection failed')
        print("Error :",e)
        time.sleep(3)


app=FastAPI()

class Post(BaseModel):
    title:str
    content:str
    published:bool=True


@app.get('/')
def home():
    return {"Welcome":"Vanga Vanga...!"}



# get all the posts
myPost=[{"title":"haha","content":"hahahahaha","id":1},{"title":"hahahaha","content":"hahahahahahahahaha","rating":4,"id":2}]
@app.get('/posts')
def get_posts():
    cur.execute(""" select * from posts""")
    post=cur.fetchall()
    print(post)
    return {"data":post}


# create a post
@app.post('/posts',status_code=status.HTTP_201_CREATED)
def create(post:Post):
    cur.execute(""" insert into posts (title,content,published) values (%s,%s,%s) returning * """,(post.title,post.content,post.published))
    new_post=cur.fetchone()
    con.commit()
    return {"data":new_post}


# get the latest post
@app.get('/post/latest')
def latest_post():
    post=myPost[len(myPost)-1]
    return {'detail':post} 


# search post with id
def find_post(id):
    for x in myPost:
        if x["id"]==id:
            return x

@app.get('/posts/{id}')
def get_post(id:int):
    cur.execute("""select * from posts where id = %s""",(str(id)))
    post_=cur.fetchone()
    if not post_:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"the following id: {id} is not found")
    return {"data":post_}


# delete a post with id
def find_posts_index(id):
    for i,p in enumerate(myPost):
        if p['id']==id:
            return i

@app.delete("/posts/{id}",status_code=status.HTTP_204_NO_CONTENT)
def delete_posts(id:int):
    cur.execute("""delete from posts where id = %s returning *""",(str(id),))
    deleted_post=cur.fetchone()
    con.commit()
    if deleted_post==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"posts with id: {id} does not exists")
    return {"message":"post deleted successfully"}


# update a post
class updateSchema(BaseModel):
    title : Optional[str] = None,
    content : Optional[str] = None,
    published : bool = True

def filter_post(post):
    pro_post=dict()          #processed post
    for i, j in post.items():
        if j != None:
            pro_post[i] = j
    post_key=list(pro_post.keys())
    return pro_post,post_key
    

@app.put('/posts/{id}')
def update_post(id: int, post:updateSchema):
    pre_post = post.dict()
    pro_post,post_key=filter_post(pre_post)
    if pro_post==None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"posts with id: {id} does not exists")
    if post_key == ['title','content','published']:
        cur.execute(""" update posts set title = %s, content = %s, published = %s where id = %s returning *;""",
        (pro_post['title'],pro_post['content'],pro_post['published'],str(id)) )
        updated_post = cur.fetchone()
        con.commit()
    elif post_key == ['title','content']:
        cur.execute(""" update posts set title = %s, content = %s where id = %s returning *;""",
        (pro_post['title'],pro_post['content'],str(id)) )
        updated_post = cur.fetchone()
        con.commit()
    elif post_key == ['title','published']:
        cur.execute(""" update posts set title = %s, published = %s where id = %s returning *;""",
        (pro_post['title'],pro_post['published'],str(id)) )
        updated_post = cur.fetchone()
        con.commit()
    elif post_key == ['content','published']:
        cur.execute(""" update posts set content = %s, published = %s where id = %s returning *;""",
        (pro_post['content'],pro_post['published'],str(id)) )
        updated_post = cur.fetchone()
        con.commit()
    elif post_key == ['title']:
        cur.execute(""" update posts set title = %s where id = %s returning *""",
        (pro_post['title'],str(id)) )
        updated_post = cur.fetchone()
        con.commit() 
    elif post_key == ['content']:
        cur.execute(""" update posts set content = %s where id = %s returning *;""",
        (pro_post['content'],str(id)) )
        updated_post = cur.fetchone()
        con.commit()
    elif post_key == ['published']:
        cur.execute(""" update posts set published = %s where id = %s returning *;""",
        (pro_post['published'],str(id)) )
        updated_post = cur.fetchone()
        con.commit()
    return {"message":f"post updated successfully..! {updated_post}"}