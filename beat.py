from scheduler import app,migrate_chunks,res

@app.on_after_configure.connect
def set_periodic_tasks(sender,**kwargs):
    sender.add_periodic_task(res["frequency"],migrate_chunks.s(res["chunk_size"],"pointer",res["schedule_id"]))