file = "spark_logs_18401.txt"

# Open the file and read the text
with open("sparklogs/" + file, "r", encoding="UTF-8") as f:
    text = f.read()
  
type(text)

text = text.replace("\n","").replace("\\n","")


def extractPhysicalPlan(log):

  import re

  # printing original string
  print("The original string is : " + text)

  # Extract substrings between brackets
  # Using regex
  res = re.findall(r'\{.*?\}', text)

  plans = []  
  for i in res:
    if "physicalPlanDescription" in i:
      print(i)
      plans.append(i)

  sub1 = '"physicalPlanDescription":'
  sub2 = ',"sparkPlanInfo"'

  physical_plan = re.findall(sub1+"(.*)"+sub2,plans[-1])[0]

  return physical_plan
