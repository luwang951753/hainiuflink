package com.zkdn.file;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-15-5:47 下午
 * @Description:
 */
public class HainiuParquetPojo {
    private String word;
    private Long count;

    public HainiuParquetPojo(){
    }

    public HainiuParquetPojo(String word, Long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

//    @Override
//    public String toString() {
//        return "HainiuParquetPojo{" +
//                "word='" + word + '\'' +
//                ", count=" + count +
//                '}';
//    }
}