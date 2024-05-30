import pandas_gbq
import pandas as pd
import email.message
import smtplib

#Baixando a tabela do GCP com a previsões
sql = """SELECT * FROM `pure-century-423811-n3.Base_de_dados.Energy_Predicted`
ORDER BY Date"""

project_id = "pure-century-423811-n3"
df = pd.read_gbq(sql, project_id=project_id, dialect="standard")


#Coletando os dados necessários para o relatório
status_list=[]
count_atention=0
count_ok=0
count_severe=0

for i in range(0,len(df)):
  if df["Predicted"][i]>1000:
    status_list.append('ATENTION')
    count_atention+=1
  elif df["Predicted"][i]> 2000:
    status_list.append('SEVERE')
    count_severe+=1
  else:
    status_list.append('OK')
    count_ok+=1


end_year = round(df["Predicted"].sum(), ndigits=2)
month_mean = round(end_year/48, ndigits=2)


#Criação da inteligência de status
if count_severe > count_ok and count_severe > count_atention:
  final_conclusion = 'Sua empresa esta completamente fora das metas, é necessário ações urgentes, rápidas e drásticas para ainda haver chance de atingir as metas ESG. Tem que agir AGORA.'
elif count_ok < count_atention and  count_ok < count_severe:
  final_conclusion = 'Minha recomendação é que sua empresa tome ações urgentemente e rápido para que ela volte para o caminho certo na busca em atingir as metas ESG. A situação é preocupante, mas ainda há tempo.'
elif count_ok > count_severe and count_ok < count_atention:
  final_conclusion = 'Sua empresa precisa de uma atenção especial, ela não está atingindo as metas mas ainda não é grave e há tempo de se organizar e voltar para o caminho certo. Foco!'
else:
  final_conclusion = 'Sua empresa está no caminho certo!! Continue assim e as metas e objetivos serão cumpridos!!'

#Criação da mensagem do email
email_body = f"""
<p>Olá! Trago minha análise de gastos até o fim do ano: </p>

<p>Sua média mensal foi de R${month_mean}</p>

<p>Sua empresa terá {count_severe} semanas muito acima do objetivo, </p>

<p>Sua empresa terá {count_atention} semanas acima do objetivo e </p>

<p>Sua empresa terá {count_ok} semanas dentro da meta. </p>

<p>Foi previsto que será gasto R${end_year} com energia elétrica até o fim do ano.</p>

<p>Conclusão Final: {final_conclusion} </p>

<p>Abs,</p>
<p>IA Preditiva</p>
"""

#Criando o objeto email
msg = email.message.Message()

#Configurarando as informações do e-mail
msg["From"] = "leo.dummy.bot@gmail.com"
msg["Subject"] = "Relatório semanal de gastos previstos pela IA com energia elétrica:"
msg["To"] = 'leorsr1@gmail.com'
password = 'ewierwzruzfdvcsu'

msg.add_header('Content-Type', 'text/html')
msg.set_payload(email_body)

#Conectando ao servidor do @gmail.com
s = smtplib.SMTP('smtp.gmail.com: 587')
s.starttls()

#Logando e enviando o email
s.login(msg['From'], password)
s.sendmail(msg['From'], [msg['To']], msg.as_string().encode('utf-8'))