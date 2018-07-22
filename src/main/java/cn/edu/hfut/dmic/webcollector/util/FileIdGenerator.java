/*
 * Copyright (C) 2017 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.util;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hu
 */
public class FileIdGenerator {
    protected File baseDir;
    protected AtomicInteger lastId;

    public void initLastId(){
        lastId = new AtomicInteger(-1);
        if(baseDir.exists()){
            for(File file:baseDir.listFiles()){
                int id = Integer.valueOf(file.getName().split("\\.")[0]);
                if(id > lastId.get()){
                    lastId.set(id);
                }
            }
        }
    }

    public int generate(){
        return lastId.incrementAndGet();
    }

    public FileIdGenerator(File baseDir) {
        this.baseDir = baseDir;
        initLastId();

    }
    public FileIdGenerator(String baseDirPath){
        this(new File(baseDirPath));
    }

    public static void main(String[] args) {
        FileIdGenerator generator = new FileIdGenerator("test");
        for(int i=0;i<100;i++){
            System.out.println(generator.generate());
        }
    }
}
