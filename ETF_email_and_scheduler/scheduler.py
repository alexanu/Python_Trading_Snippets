# Scheduler imports
from apscheduler.schedulers.blocking import BlockingScheduler

# Job imports
import main as job
from emails import email_login

def bob_job():

    me, password = email_login()
    sched = BlockingScheduler()

    @sched.scheduled_job('cron', day_of_week='mon,tue,wed,thu,fri', hour=17)
    def scheduled_job():
        job.run(me, password)

    sched.start()    


if __name__ == '__main__':
    bob_job()
