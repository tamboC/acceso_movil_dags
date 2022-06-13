from airflow.operators.email_operator import EmailOperator

def send_email(emailPara = [], asunto = None, texto = None, adjunto = None, **context):
   
    email_op = EmailOperator(
            task_id      = 'send_email',
            to           = emailPara,
            subject      = asunto,
            html_content = texto,
            files = adjunto if adjunto == None else [adjunto],
        )
    print("Se envió email")
    return email_op.execute(context)