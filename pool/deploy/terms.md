# The pool's terms: a draft to publish

This is a draft of the pool's terms, written to match how the pool works. It is not legal advice. Before you publish it:

1. Fill in the three placeholders in square brackets: who operates the pool, how to reach them, and which law governs.
2. Have a lawyer read it if you can, sections 9 to 11 above all.

To publish: on the admin site open **Terms**, choose **The pool's terms** (published in every region), paste the text between the two lines below, and publish. The sites show the terms exactly as typed, as plain text, so the text uses no Markdown. Publishing the same text again changes nothing; any change is a new version.

Sections 4 and 5 describe the commission and the payout window without naming their values, because the sites show the values in force and you can change them in **Settings** without publishing new terms.

```text
OPEN POOL TERMS

These are the terms of Open Pool, a pool of GPU machines that serves AI and video apps on the Livepeer network. The pool is operated by [OPERATOR NAME] ("the pool", "we"). You can reach us at [CONTACT].

By applying to join, you accept these terms. Your application is signed with your Ethereum wallet, and that signature records the version of the terms you accepted. A region of the pool may have terms of its own, which apply in that region in addition to these.


1. WHAT THE POOL IS

Members connect their own machines to the pool. The pool runs the Livepeer orchestrator that customers send work to, takes their payments, decides which app each machine runs, tests the machines, and pays members for the work their machines did.

The pool is made of regions. Each region is a separate membership with its own machines, earnings and payouts.


2. JOINING

2.1 You apply on the member site by signing in with an Ethereum wallet, choosing one or more regions, and listing your machines and their GPUs. We read every application and may approve or reject it, for any reason.

2.2 The address you sign in with is your account and the address you are paid at. It cannot be changed. A different address is a different membership and needs its own application.

2.3 You must own each machine you connect, or have its owner's permission to use it this way. You must be allowed to do this work and to be paid for it under the laws that apply to you.

2.4 What you tell us about your machines must be true. The agent reports the GPUs it finds, and we may reject or remove a machine that is not what was declared.


3. YOUR MACHINES AND THE AGENT

3.1 To connect a machine you run the pool's agent on it, with a credential that the member site creates for that machine. Keep the credential secret and use it on that machine only. If it may have been seen by someone else, replace it on the member site.

3.2 The agent has access to the machine's Docker daemon. That is the same as full control of the machine. It uses that access to start and stop the containers the pool assigns to the machine: the apps customers use, and the tunnel that connects them to the orchestrator. The agent acts only on instructions signed by the pool, and its source code is public. Do not install the agent on a machine that holds anything you would not trust us with.

3.3 We decide which apps each machine runs and when their versions change. The agent applies those changes by itself. An app may download large models and use the machine's GPUs, memory, disk and network fully while it runs.

3.4 You are responsible for your machines: their hardware, electricity, cooling, internet connection, security, and any cost or damage that comes from running them. We do not pay for any of these.

3.5 You may remove a machine or stop the agent at any time. Sessions a machine is serving are allowed to finish first where that is possible.


4. TESTS AND WORK

4.1 We test every machine with real requests before it takes customers' work, and keep testing it while it is connected. A machine that fails tests is suspended until it passes again. Tests are not paid.

4.2 We do not promise any amount of work, or any work at all. How much a machine earns depends on what customers ask for, which apps it runs, how reliably it answers, and how many other machines are connected.

4.3 Prices are set by the pool for each app and region and are shown on the public site. They can change at any time.


5. HOW YOU ARE PAID

5.1 Payouts are made per payout window, which is a fixed number of Livepeer rounds, shown on the public site for each region.

5.2 When a window closes, the pool takes what the region's orchestrator actually redeemed on-chain during the window, keeps the pool's commission, and splits the rest among members in proportion to the fees their machines earned in that window. The commission is the one shown on the public site when the window closes.

5.3 Livepeer pays orchestrators with probabilistic tickets, so the amount redeemed in a window is partly chance. It can be more or less than the fees customers were charged in that window, and it can be nothing. You are paid your share of what was redeemed, not the fees your machines are shown to have earned.

5.4 Payouts are sent in ETH on Arbitrum One to the address you signed in with. There is no minimum. The pool pays the transaction fees. Amounts are rounded down to the smallest unit.

5.5 A payout may wait for our review before it is sent. If a payment cannot be delivered, for example because your address does not accept ETH, we may try again, but we are not responsible for payments your address cannot receive.

5.6 If we find that earnings were recorded in error, or were obtained by breaking these terms, we may leave them out of a payout.

5.7 You are responsible for any tax on what you are paid.


6. WHAT YOU MUST NOT DO

You must not:

(a) read, keep, copy or share anything customers send to or receive from an app on your machine;
(b) change, replace or interfere with the containers, images, tunnel or agent the pool runs on your machine, or return results that the app did not really produce;
(c) make or cause requests to the pool in order to earn fees from them, or collude with a customer to do so;
(d) run the same credential on more than one machine, or share a machine between memberships;
(e) attack, probe or overload the pool, its customers or other members;
(f) use the pool for anything unlawful.


7. SUSPENSION AND BANS

7.1 We may suspend a machine or ban a member at any time, for any reason, without notice. Breaking section 6, or any use we judge to be malicious, is an immediate ban.

7.2 A ban removes all of the member's machines from the pool at once. A member banned during a payout window is not paid for that window unless we decide otherwise.

7.3 You may leave the pool at any time by removing your machines. Fees your machines earned in a window that is still open are paid when it closes, provided you were not banned.


8. WHAT WE RECORD AND WHAT IS PUBLIC

8.1 We record your Ethereum address, the display name you chose, your machines and their GPUs, what the agent reports about each machine (the GPUs it finds, its version, and the state of the containers), the network address your machines connect from, test results, earnings and payouts. We do not ask for your name or email address.

8.2 The public site shows a leaderboard with each member's display name, a shortened form of their address, and their earnings. Payouts are transactions on a public blockchain and can be seen by anyone.

8.3 We use what we record to run the pool: to assign work, test machines, pay members, and investigate abuse.


9. NO WARRANTY

The pool, the agent and the sites are provided as they are. We do not promise that they will be available, free of errors, or secure, or that the Livepeer network, Arbitrum, or any app will work as expected. Software the pool runs on your machine comes from third parties; we choose it with care but do not warrant it.


10. LIMIT OF LIABILITY

To the extent the law allows, we are not liable for lost earnings, lost data, damage to hardware, or any indirect or consequential loss arising from your use of the pool. Our total liability to you is limited to the amount the pool has paid you in the three months before the claim arose.


11. YOUR RELATIONSHIP WITH THE POOL

You take part as an independent provider of computing capacity. Nothing in these terms makes you our employee, partner or agent. Membership is not an investment and gives no share in the pool or its operator.


12. CHANGES

We may change these terms by publishing a new version on the sites. A new version applies from the moment it is published. If you do not accept it, remove your machines; keeping them connected means you accept it.


13. LAW

These terms are governed by the laws of [GOVERNING LAW].
```
