package org.bilan.clock.job;

import org.bilan.common.consts.RecordConstants;
import org.bilan.domain.Activity;
import org.bilan.domain.ActivityParticipation;
import org.bilan.service.ActivityParticipationDetailService;
import org.bilan.service.ActivityParticipationService;
import org.bilan.service.ActivityService;
import org.bilan.utils.DateUtils;
import org.bilan.utils.UserIncomeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author LiBin
 * DateTime 2019/1/22 21:36
 * Description
 */

@Component
public class SeparateAccountsExecutor {

    Logger logger = LoggerFactory.getLogger(SeparateAccountsExecutor.class);

    @Autowired
    private ActivityService activityService;

    @Autowired
    private ActivityParticipationService participationService;

    @Autowired
    private ActivityParticipationDetailService detailService;

    @Autowired
    private UserIncomeUtil userIncomeUtil;

    private BigDecimal rate = BigDecimal.valueOf(0.1);

    // 测试 任务结束计算完奖金后未调整任务状态
    // 计算时出去在任务结束之前就已经失败的用户 不扣他们的金额

    //  分账逻辑
    //分账跑定时任务，每天三次
    @Scheduled(cron = "0 0 0/3 * * ?")
//    @Scheduled(cron = "0/10 * * * * ? ")
    public void giveCard() {
        logger.info("分账");
        try{
            //获取所有未分账任务
            List<Activity> activities = activityService.getNotAccountsActivity();

            activities.forEach(this::process);
        }catch (Exception ex){
            logger.error(ex.getMessage());
        }

    }


    @Transactional
    public void process(Activity activity) {
        // 2 遍历 获取单个任务参与信息
        List<ActivityParticipation> participations = participationService.getByActivity(activity.getId());
        // 计算总奖金池
        BigDecimal amount = participations.stream().map(ActivityParticipation::getPayAmount).reduce(BigDecimal.ZERO, BigDecimal::add);

        logger.info("任务 {} 的总奖金 {}", activity.getId(), amount);

        // 计算该任务需要的打卡次数
        LocalDateTime start = activity.getBeginTime().toLocalDateTime();
        LocalDateTime end = activity.getEndTime().toLocalDateTime();
        Integer maxNeedNum = this.getNeedNum(start, end, activity.getWeekdays(),null);

        //计算每个参加的用户实际打卡次数
        List<ActivityParticipation> current = participations.stream().filter(o -> o.getStatus()==1).collect(Collectors.toList());
        current.forEach(o -> {
            Integer needNum = this.getNeedNum(o.getCreatedAt().toLocalDateTime(), end, activity.getWeekdays(), activity.getTime());
            o.setNeedNum(needNum);
            o.setStatus(detailService.getNumByUser(activity.getId(), o.getUserId()) < needNum ? 0 : o.getStatus());
        });

        if (!CollectionUtils.isEmpty(participations)) {
            participationService.merge(participations);
        }

        Integer totalNeedNum = current.stream().filter(o -> o.getStatus() == 1).map(ActivityParticipation::getNeedNum).reduce(0,Integer::sum);

        //  计算未完成总奖金
        BigDecimal currentAmount = participations.stream().filter(o -> o.getStatus() == 0).map(ActivityParticipation::getPayAmount).reduce(BigDecimal.ZERO, BigDecimal::add);

        //完成打卡金额
        BigDecimal finishAmount = amount.subtract(currentAmount);

        //都未完成
        if(BigDecimal.ZERO.compareTo(finishAmount) == 0){
            this.processFail(activity,current);
        }
        //都完成
        else if(BigDecimal.ZERO.compareTo(currentAmount) == 0){
            this.processSuccess(activity,current,BigDecimal.ZERO);
        }
        //一部分完成一部分未完成
        else {
            //瓜分份数
            BigDecimal num = finishAmount.divide(activity.getDepositAmount(), 0).multiply(BigDecimal.valueOf(totalNeedNum));
            // 获取抽成比例 每份金额
            BigDecimal partAmount = (currentAmount.multiply(BigDecimal.ONE.subtract(rate))).divide(num, 2);
            //已完成处理
            this.processSuccess(activity,current,partAmount);
            // 未完成的人通知
            this.processFail(activity,current);
        }
        activity.setAccounts(2);
        activityService.merge(activity);
    }

    private void processFail(Activity activity, List<ActivityParticipation> participation){
        if(CollectionUtils.isEmpty(participation)){
            return;
        }
        // 未完成的人记录 通知
        participation.stream().filter(o -> o.getStatus() == 0).forEach(item ->
                // 记录 同步 通知
                userIncomeUtil.FailAndRecord(item.getUserId(),item.getPayAmount(),
                        RecordConstants.DIVIDE_AMOUNT_TITLE,
                        String.format(RecordConstants.DIVIDE_AMOUNT_CONTENT,activity.getName(),item.getPayAmount())));
    }

    private void processSuccess(Activity activity, List<ActivityParticipation> participation, BigDecimal perAmount){
        if(CollectionUtils.isEmpty(participation)){
            return;
        }
        participation.stream().filter(o -> o.getStatus() == 1).forEach(item -> {
            BigDecimal personAmount ;
            BigDecimal userAmount ;

            if(BigDecimal.ZERO.compareTo(perAmount) == 0){
                personAmount = BigDecimal.ZERO;
                userAmount = item.getPayAmount();
            }else {
                personAmount = item.getPayAmount().divide(activity.getDepositAmount(), 0).multiply(perAmount).multiply(BigDecimal.valueOf(item.getNeedNum()));
                userAmount = item.getPayAmount().add(personAmount);
            }
            // 记录 同步 通知
            userIncomeUtil.IncomeAndRecord(item.getUserId(),activity.getId(),userAmount, personAmount,
                    RecordConstants.GET_AMOUNT_TITLE,
                    String.format(RecordConstants.GET_AMOUNT_CONTENT,activity.getName(),userAmount));
        });
    }


    private Integer getNeedNum(LocalDateTime start, LocalDateTime end, String weeks, String time) {
        List<String> days = Arrays.asList(weeks.split(","));
        List<DayOfWeek> dayOfWeeks = new ArrayList<>();
        days.forEach(d -> {
            switch (d) {
                case "周一":
                    dayOfWeeks.add(DayOfWeek.MONDAY);
                    break;
                case "周二":
                    dayOfWeeks.add(DayOfWeek.TUESDAY);
                    break;
                case "周三":
                    dayOfWeeks.add(DayOfWeek.WEDNESDAY);
                    break;
                case "周四":
                    dayOfWeeks.add(DayOfWeek.THURSDAY);
                    break;
                case "周五":
                    dayOfWeeks.add(DayOfWeek.FRIDAY);
                    break;
                case "周六":
                    dayOfWeeks.add(DayOfWeek.SATURDAY);
                    break;
                case "周日":
                    dayOfWeeks.add(DayOfWeek.SUNDAY);
                    break;
            }
        });
        Integer count = 0;

        do {
            if (dayOfWeeks.contains(start.getDayOfWeek())) {

                if(StringUtils.isEmpty(time)){
                    count++;
                }else {
                    Date now = new Date();
                    String[] times = time.split("-");
                    String startTimeStr = DateUtils.dateToString(now, true) + " " + times[0] + ":00";
                    LocalDateTime startTime = DateUtils.date2LocalDateTime(DateUtils.stringToDate(startTimeStr, false));
                    if(start.toLocalTime().isBefore(startTime.toLocalTime())){
                        count++;
                    }
                }


            }
            start = start.plusDays(1);
        } while (start.isEqual(end) || start.isBefore(end));

        return count;
    }
}
