create table course as
select distinct
    "subj" as subject,
    "#" as number,
from
    semester_data;

create table section as
with
    all_section as (
        select distinct
            "subj" as subject,
            "#" as number,
            "Comp Numb" as crn,
            "Sec" as sec,
            "Max Enrollment" as max_enrollment,
            "Current Enrollment" as current_enrollment
        from
            semester_data d
    )
select
    c.subject,
    c.number,
    crn,
    sec,
    max_enrollment,
    current_enrollment
from
    all_section d
    left join course c on d.subject = c.subject
    and d.number = c.number;


create table enrollment as (
    select
        json_object(
            'subject', c.subject,
            'number', c.number,
            'sections', coalesce(
                json_group_array(
                    json_object(
                        'crn', s.crn,
                        'sec', s.sec,
                        'max_enrollment', s.max_enrollment,
                        'current_enrollment', s.current_enrollment
                    )
                ),
                []
            )
        ) as 'course'
    from
        course c left join section s
        on c.subject = s.subject and c.number = s.number
    group by
        c.subject, c.number
);
