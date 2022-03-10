package com.zkdn.warehouse.dwd.warehousemodel;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-02-10:40 上午
 * @Description:
 */
public class PageSupport<T> {
    private String type;
    private String message;
    private T items;
    private Integer totalCount = 0;
    private Integer pageCount = 0;
    private Integer pageSize = 10;


    public PageSupport(String type, String message, T items, Integer totalCount, Integer pageCount, Integer pageSize, Integer curPage, Integer num) {
        this.type = type;
        this.message = message;
        this.items = items;
        this.totalCount = totalCount;
        this.pageCount = pageCount;
        this.pageSize = pageSize;
        this.curPage = curPage;
        this.num = num;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getItems() {
        return items;
    }

    public void setItems(T items) {
        this.items = items;
    }

    public Integer getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }

    public Integer getPageCount() {
        return pageCount;
    }

    public void setPageCount(Integer pageCount) {
        this.pageCount = pageCount;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getCurPage() {
        return curPage;
    }

    public void setCurPage(Integer curPage) {
        this.curPage = curPage;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    private Integer curPage = 1;
    private Integer num = 5;


}
