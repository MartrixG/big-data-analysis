f_val = open('val.txt')
f_val_test = open('贝叶斯val.txt')
lines_val = f_val.readlines()
lines_val_test = f_val_test.readlines()
answer = []
test = []
for line in lines_val:
    answer.append(line[-2])

for line in lines_val_test:
    test.append(line[-2])

tp = tn = fp = fn = 0.0
for i in range(300000):
    if(test[i] == '1' and answer[i] == '1'):
        tp += 1
    if(test[i] == '0' and answer[i] == '0'):
        tn += 1
    if(test[i] == '1' and answer[i] == '0'):
        fp += 1
    if(test[i] == '0' and answer[i] == '1'):
        fn += 1
acc = (tp + tn) / (tp + tn + fp + fn)
pre = tp / (tp + fp)
rec = tp / (tp + fn)
f1 = 2 * pre * rec / (pre + rec)

print(acc, pre, rec, f1)
